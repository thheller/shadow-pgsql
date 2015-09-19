(ns shadow.pgsql.benchmark.runner
  (:require [shadow.pgsql :as sql]
            [criterium.core :as crit])
  (:import (shadow.pgsql.benchmark JDBCBenchmark ShadowBenchmark)))

(set! *warn-on-reflection* true)

(def test-rows 10000)

(defn generate-pojo-data []
  (->> (range test-rows)
       (map (fn [i]
              {:test-int i
               :test-long i
               :test-string (str "dat-string-" i)
               :test-double (* i (rand))
               :test-bd (* i 123456789M)}))
       (into [])))


(defn insert-test-data []
  (let [db (sql/start {:host "localhost"
                       :port 5432
                       :user "zilence"
                       :database "shadow_bench"})]

    (sql/execute db "DELETE FROM pojos")
    (sql/insert
      db
      {:table :pojos
       :columns [:test-int
                 :test-long
                 :test-double
                 :test-string
                 :test-bd]}
      (generate-pojo-data))
    (sql/stop db)
    ))


(defn check-size [coll]
  (when (not= test-rows (count coll))
    (throw (ex-info "didnt read all rows" {:c (count coll)
                                           :t test-rows}))))

(defn bench-jdbc []
  (let [db (JDBCBenchmark. "blubb")]
    (crit/bench
      (check-size (.selectPojos db))
      :verbose)))

(defn bench-java []
  (let [it (ShadowBenchmark.)]
    (crit/bench
      (check-size (.selectPojos it))
      :verbose)
    ))

(defn bench-clojure []
  (let [db (sql/start {:user "zilence"
                       :database "shadow_bench"})]
    (crit/bench
      (check-size (sql/query db "SELECT * FROM pojos"))
      :verbose)))


(defn -main [which-one & args]
  (case which-one
    "pgjdbc"
    (do (println "===== pgjdbc via java")
        (bench-jdbc))
    "clojure"
    (do (println "===== shadow-pgsql via clojure")
        (bench-clojure))
    "java"
    (do (println "===== shadow-pgsql via java")
        (bench-java))
    (prn "which one to run: pgjdbc, clojure, java")))
