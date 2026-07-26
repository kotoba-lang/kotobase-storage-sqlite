(ns kotobase.storage.sqlite-test
  (:require [clojure.test :refer [deftest is testing]]
            [kotobase.engine :as engine]
            [kotobase.storage.contract :as contract]
            [kotobase.storage.core :as storage]
            [kotobase.storage.sqlite :as sqlite])
  (:import [java.nio.file Files Path]
           [java.nio.file.attribute FileAttribute]))

(defn- temp-database []
  (let [directory (Files/createTempDirectory
                   "kotobase-sqlite-"
                   (make-array FileAttribute 0))]
    {:directory directory
     :path (.resolve ^Path directory "storage.db")}))

(defn- delete-database! [{:keys [^Path directory ^Path path]}]
  (doseq [suffix ["-shm" "-wal" ""]]
    (Files/deleteIfExists
     (Path/of (str path suffix) (make-array String 0))))
  (Files/deleteIfExists directory))

(defn- with-database [run!]
  (let [{:keys [path] :as temporary} (temp-database)]
    (try
      (run! path)
      (finally
        (delete-database! temporary)))))

(deftest shared-storage-contract
  (with-database
    (fn [path]
      (let [checks (atom [])
            backend (sqlite/open {:path path})]
        (contract/verify backend
                         (fn [truthy label]
                           (swap! checks conj label)
                           (is truthy label)))
        (is (= 8 (count @checks)))))))

(deftest cid-collision-is-rejected-without-changing-stored-bytes
  (with-database
    (fn [path]
      (let [backend (sqlite/open {:path path})]
        (storage/put-block! backend "same-cid" (byte-array [1]))
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"different bytes"
             (storage/put-block! backend "same-cid" (byte-array [2]))))
        (is (= [1] (vec (storage/get-block backend "same-cid"))))))))

(deftest competing-genesis-cas-has-one-winner
  (with-database
    (fn [path]
      (let [backend (sqlite/open {:path path})
            start (promise)
            attempt (fn [cid]
                      (future
                        @start
                        (storage/-compare-and-set-ref!
                         backend "race" nil cid)))
            left (attempt "left")
            right (attempt "right")]
        (deliver start true)
        (let [results [@left @right]]
          (is (= 1 (count (filter :published? results))))
          (is (contains? #{"left" "right"}
                         (:cid (storage/-read-ref backend "race")))))))))

(deftest engine-data-persists-across-reopen
  (with-database
    (fn [path]
      (let [open-engine
            (fn []
              (engine/open
               {:storage (sqlite/open {:path path})
                :encrypt-fn identity
                :decrypt-fn identity
                :blind-fn pr-str
                :visible? (constantly true)}))
            first-database (open-engine)
            committed (engine/transact!
                       first-database [["alice" "role" "admin"]])
            reopened (open-engine)]
        (testing "the mutable head and immutable block chain survive reopen"
          (is (= committed (engine/head reopened)))
          (is (= #{{:s "alice" :p "role" :o "admin"}}
                 (engine/q reopened ["alice" "role" nil]))))))))

(deftest invalid-open-options-fail-closed
  (is (thrown? clojure.lang.ExceptionInfo (sqlite/open {})))
  (is (thrown? clojure.lang.ExceptionInfo
               (sqlite/open {:path "ignored.db" :busy-timeout-ms -1}))))

