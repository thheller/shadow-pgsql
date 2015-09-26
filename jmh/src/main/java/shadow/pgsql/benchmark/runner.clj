(ns shadow.pgsql.benchmark.runner
  (:require [shadow.pgsql :as sql]
            [criterium.core :as crit]
            [clojure.java.jdbc :as jdbc])
  (:import (shadow.pgsql.benchmark JDBCBenchmark ShadowBenchmark)))

(set! *warn-on-reflection* true)

(defn generate-pojo-data [amount]
  (->> (range amount)
       (map (fn [i]
              {:test-int i
               :test-long i
               :test-string (str "dat-string-" i)
               :test-double (* i (rand))
               :test-bd (* i 123456789M)}))))


(defn insert-test-data [amount]
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
      (generate-pojo-data amount))
    (sql/execute db "VACUUM pojos")
    (sql/stop db)
    ))


(defn check-size [coll amount]
  (when (not= amount (count coll))
    (throw (ex-info "didnt read all rows" {:c (count coll)
                                           :t amount}))))

(defn bench-jdbc [amount]
  (let [db (JDBCBenchmark. "blubb")]
    (crit/bench
      (check-size (.selectPojos db) amount)
      :verbose)))

(defn bench-java [amount]
  (let [it (ShadowBenchmark.)]
    (crit/bench
      (check-size (.selectPojos it) amount)
      :verbose)
    ))

(defn bench-clojure [amount]
  (let [db (sql/start {:user "zilence"
                       :database "shadow_bench"})]
    (crit/bench
      (check-size (sql/query db "SELECT * FROM pojos") amount)
      :verbose)))

(defn bench-clojure-jdbc [amount]
  (let [db {:connection-uri "jdbc:postgresql://localhost:5432/shadow_bench"
            :user "zilence"
            :password ""}
        con (jdbc/get-connection db)
        db (assoc db :connection con)]

    (crit/bench
      (check-size
        (jdbc/query db ["select * from pojos"])
        amount)
      :verbose)))


(defn -main [amount which-one & args]
  (let [amount (Long/parseLong amount)]
    (insert-test-data amount)
    (case which-one
      "pgjdbc"
      (do (println "===== pgjdbc via java")
          (bench-jdbc amount))
      "clojure-jdbc"
      (do (println "===== clojure.java.jdbc")
          (bench-clojure-jdbc amount))
      "clojure"
      (do (println "===== shadow-pgsql via clojure")
          (bench-clojure amount))
      "java"
      (do (println "===== shadow-pgsql via java")
          (bench-java amount))
      (prn "which one to run: pgjdbc, clojure, java"))))
