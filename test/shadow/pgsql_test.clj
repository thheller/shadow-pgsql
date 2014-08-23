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

(defn test-db []
  (sql/start {:user "zilence" :database "shadow_pgsql"}))

(deftest test-basics
  (with-open [db (test-db)]
    (pprint
      (sql/execute db "DELETE FROM num_types"))

    (pprint
      (sql/insert db ?insert-num-types [{:fint2 123
                                         :something :ignored}]))

    (pprint (sql/query db ?select-fint2))
    ))


(def keyword-set-type
  (sql/set-type sql/keyword-type))

(deftest test-arrays
  (with-open [db (-> (sql/start {:user "zilence" :database "shadow_pgsql"})
                     (sql/with-types {:array-types {:atext keyword-set-type}})
                     )]

    (sql/execute db "DELETE FROM array_types")

    (sql/execute db {:sql "INSERT INTO array_types (atext) VALUES ($1)"
                     :params [keyword-set-type]}
                 #{:clojure :postgresql :java})

    (pprint (sql/query db "SELECT atext FROM array_types"))

    (pprint (sql/query db {:sql "SELECT atext FROM array_types WHERE atext @> ARRAY[$1]"
                           :params [sql/keyword-type]}
                       :clojure
                       ))

    ))

(deftest test-prepare
  (with-open [db (test-db)]
    (sql/with-connection db
      (with-open [insert (sql/prepare db "INSERT INTO types (t_bool, t_int4, t_text) VALUES ($1, $2, $3)") ]
        (pprint (insert true 1 "hello"))
        (pprint (insert false 2 "world"))))
    ))
