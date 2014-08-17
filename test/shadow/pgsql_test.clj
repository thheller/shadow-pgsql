(ns shadow.pgsql-test
  (:use [clojure.test])
  (:import [shadow.pgsql Helpers])
  (:require [clojure.pprint :refer (pprint)]
            [shadow.pgsql :as sql]))

(def ?insert-num-types
  {:table :num-types
   :columns [:fint2]
   :columns-fn (fn [row columns]
                 (mapv #(get row %) columns))
   :returning [:id]

   :merge-fn (fn [row result]
               (assoc row :id result))

   :row sql/row->one-column
   :result sql/result->one-row})

(def ?select-fint2
  {:sql "SELECT fint2 FROM num_types"
   :params []
   :result sql/result->vec
   :row sql/row->map})

(deftest test-basics
  (with-open [db (sql/start {:user "zilence" :database "shadow_pgsql"})]
    (pprint
     (sql/execute db "DELETE FROM num_types"))

    (pprint
     (sql/insert db ?insert-num-types [{:fint2 123
                                        :something :ignored}]))

    (pprint (sql/query db ?select-fint2))
    ))

(deftest ^:wip test-with-actual-data
  (with-open [db (-> (sql/start {:user "zilence" :database "cms"})
                     (sql/with-types {:cms-public {:a sql/keyword-type}}))]

    (let [data (sql/query db {:sql "SELECT * FROM cms_public"
                              :result sql/result->vec
                              :row sql/row->map})]
      (prn [:count (count data)]))
    ))
