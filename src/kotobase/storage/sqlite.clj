(ns kotobase.storage.sqlite
  "SQLite implementation of immutable blocks and linearizable local refs."
  (:require [clojure.string :as str]
            [kotobase.storage.core :as storage])
  (:import [java.sql Connection PreparedStatement ResultSet]
           [java.util Arrays]
           [javax.sql DataSource]
           [org.sqlite SQLiteDataSource]))

(def schema-statements
  ["CREATE TABLE IF NOT EXISTS kotobase_blocks (
      cid TEXT PRIMARY KEY,
      bytes BLOB NOT NULL,
      byte_length INTEGER NOT NULL CHECK (byte_length >= 0),
      created_at INTEGER NOT NULL DEFAULT (unixepoch())
    ) WITHOUT ROWID"
   "CREATE TABLE IF NOT EXISTS kotobase_refs (
      name TEXT PRIMARY KEY,
      cid TEXT NOT NULL,
      revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
      updated_at INTEGER NOT NULL DEFAULT (unixepoch())
    ) WITHOUT ROWID"])

(defn datasource
  "Create an unpooled SQLite DataSource from a file path or jdbc:sqlite: URL."
  ^SQLiteDataSource [path-or-url]
  (let [value (str path-or-url)
        url (if (str/starts-with? value "jdbc:sqlite:")
              value
              (str "jdbc:sqlite:" value))]
    (doto (SQLiteDataSource.)
      (.setUrl url))))

(defn- execute-statement! [^Connection connection sql]
  (with-open [statement (.createStatement connection)]
    (.execute statement sql)))

(defn- configure-connection!
  [^Connection connection busy-timeout-ms]
  (execute-statement! connection "PRAGMA foreign_keys = ON")
  (execute-statement!
   connection
   (str "PRAGMA busy_timeout = " (long busy-timeout-ms)))
  connection)

(defn- bind! [^PreparedStatement statement params]
  (doseq [[index value] (map-indexed vector params)]
    (if (bytes? value)
      (.setBytes statement (inc index) value)
      (.setObject statement (inc index) value)))
  statement)

(defn- execute! [^Connection connection sql params]
  (with-open [statement (bind! (.prepareStatement connection sql) params)]
    (.executeUpdate statement)))

(defn- rows [^Connection connection sql params row-fn]
  (with-open [statement (bind! (.prepareStatement connection sql) params)
              result (.executeQuery statement)]
    (loop [out []]
      (if (.next result)
        (recur (conj out (row-fn result)))
        out))))

(defn initialize!
  ([datasource] (initialize! datasource 5000))
  ([^DataSource datasource busy-timeout-ms]
   (with-open [connection (.getConnection datasource)]
     (configure-connection! connection busy-timeout-ms)
     ;; WAL is persistent database state and must be selected outside a
     ;; transaction. It allows readers to proceed while a writer commits.
     (execute-statement! connection "PRAGMA journal_mode = WAL")
     (doseq [statement schema-statements]
       (execute-statement! connection statement)))
   datasource))

(defn- open-connection
  ^Connection [^DataSource datasource busy-timeout-ms]
  (doto (.getConnection datasource)
    (configure-connection! busy-timeout-ms)))

(defrecord SQLiteStorage [^DataSource datasource busy-timeout-ms]
  storage/IBlockStore
  (-put-blocks! [_ blocks]
    (with-open [connection (open-connection datasource busy-timeout-ms)]
      (let [auto-commit? (.getAutoCommit connection)]
        (try
          (.setAutoCommit connection false)
          (doseq [{:keys [cid bytes]} blocks]
            (when-not (and (string? cid) (bytes? bytes))
              (throw
               (ex-info "SQLite blocks require a string CID and byte array"
                        {:type :kotobase.storage/invalid-block :cid cid})))
            (execute!
             connection
             "INSERT INTO kotobase_blocks(cid, bytes, byte_length)
              VALUES (?, ?, ?) ON CONFLICT(cid) DO NOTHING"
             [cid bytes (alength ^bytes bytes)]))
          (doseq [{:keys [cid bytes]} blocks
                  :let [stored
                        (first
                         (rows connection
                               "SELECT bytes FROM kotobase_blocks WHERE cid = ?"
                               [cid]
                               #(.getBytes ^ResultSet % 1)))]
                  :when (not (Arrays/equals ^bytes bytes ^bytes stored))]
            (throw
             (ex-info "CID already has different bytes"
                      {:type :kotobase.storage/cid-collision :cid cid})))
          (.commit connection)
          (mapv :cid blocks)
          (catch Throwable error
            (.rollback connection)
            (throw error))
          (finally
            (.setAutoCommit connection auto-commit?))))))
  (-get-blocks [_ cids]
    (if (empty? cids)
      {}
      (with-open [connection (open-connection datasource busy-timeout-ms)]
        (let [placeholders (str/join "," (repeat (count cids) "?"))]
          (into {}
                (rows connection
                      (str "SELECT cid, bytes FROM kotobase_blocks WHERE cid IN ("
                           placeholders ")")
                      cids
                      (fn [^ResultSet result]
                        [(.getString result 1) (.getBytes result 2)])))))))

  storage/IRefStore
  (-read-ref [_ name]
    (with-open [connection (open-connection datasource busy-timeout-ms)]
      (first
       (rows connection
             "SELECT cid, revision FROM kotobase_refs WHERE name = ?"
             [name]
             (fn [^ResultSet result]
               {:cid (.getString result 1)
                :version (.getLong result 2)})))))
  (-compare-and-set-ref! [this name expected next]
    (with-open [connection (open-connection datasource busy-timeout-ms)]
      (let [published
            (first
             (rows
              connection
              (if (nil? expected)
                "INSERT INTO kotobase_refs(name, cid) VALUES (?, ?)
                 ON CONFLICT(name) DO NOTHING RETURNING cid, revision"
                "UPDATE kotobase_refs
                 SET cid = ?, revision = revision + 1,
                     updated_at = unixepoch()
                 WHERE name = ? AND cid = ? RETURNING cid, revision")
              (if (nil? expected)
                [name next]
                [next name expected])
              (fn [^ResultSet result]
                {:published? true
                 :current (.getString result 1)
                 :version (.getLong result 2)})))]
        (or published
            (let [current (storage/-read-ref this name)]
              {:published? false
               :current (:cid current)
               :version (:version current)})))))

  storage/IBackendCapabilities
  (-capabilities [_]
    #{:immutable-blocks :cid-addressed-read :conditional-ref
      :linearizable-ref :batch-get :batch-put :local-file
      :sqlite-transaction}))

(defn open
  "Open a SQLite backend.

  Supply either `:datasource` or `:path`. Schema initialization defaults to
  true. `:busy-timeout-ms` controls how long competing writers wait."
  [{:keys [datasource path initialize? busy-timeout-ms]
    :or {initialize? true busy-timeout-ms 5000}}]
  (when-not (and (integer? busy-timeout-ms) (<= 0 busy-timeout-ms))
    (throw
     (ex-info "SQLite busy timeout must be a non-negative integer"
              {:type :kotobase.storage/invalid-configuration
               :backend :sqlite})))
  (let [datasource (or datasource (when path (kotobase.storage.sqlite/datasource
                                               path)))]
    (when-not (instance? DataSource datasource)
      (throw
       (ex-info "SQLite storage requires :path or javax.sql.DataSource"
                {:type :kotobase.storage/invalid-configuration
                 :backend :sqlite})))
    (when initialize?
      (initialize! datasource busy-timeout-ms))
    (->SQLiteStorage datasource busy-timeout-ms)))

