(ns shadow.pgsql-test
  (:use [clojure.test])
  (:require [clojure.pprint :refer (pprint)]
            [shadow.pgsql :as sql]))

(deftest ^:wip test-basics
  (with-open [db (sql/start {:user "zilence" :database "shadow_pgsql"})]
    (pprint
     (sql/insert db {:table :num-types
                     :columns [:fint2]
                     :columns-fn (fn [row columns]
                                   (mapv #(get row %) columns))
                     :returning [:id]

                     :merge-fn #(assoc %1 :id %2)
                     :row sql/row->single-column
                     :result sql/result->single-row}

                 [{:fint2 123
                   :something :ignored}]))

    (pprint (sql/query db {:sql "SELECT fint2 FROM num_types"
                           :params []
                           :result sql/result->vec
                           :row sql/row->map}))
    ))
