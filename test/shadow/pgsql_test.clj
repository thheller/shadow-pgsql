(ns shadow.pgsql-test
  (:use [clojure.test])
  (:import [shadow.pgsql Helpers])
  (:require [clojure.pprint :refer (pprint)]
            [shadow.pgsql :as sql]))

(def ?insert-num-types
  {:table :num-types
   :columns [:fint2]
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
                     (sql/with-types {:array-types {:atext keyword-set-type}}))]

    (sql/execute db "DELETE FROM array_types")

    (sql/execute db {:sql "INSERT INTO array_types (atext) VALUES ($1)"
                     :params [keyword-set-type]}
                 [#{:clojure :postgresql :java}])

    (pprint (sql/query db "SELECT atext FROM array_types"))

    (pprint (sql/query db
                       {:sql "SELECT atext FROM array_types WHERE atext @> ARRAY[$1]"
                        :params [sql/keyword-type]}
                       [:clojure]))

    ))

(deftest test-prepare
  (with-open [db (test-db)]
    (sql/with-connection db
                         (with-open [insert (sql/prepare db "INSERT INTO types (t_bool, t_int4, t_text) VALUES ($1, $2, $3)")]
                           (pprint (insert true 1 "hello"))
                           (pprint (insert false 2 "world"))))
    ))


(def ?update-add-one
  {:sql "UPDATE types SET t_int2 = t_int2 + 1 WHERE id = $1 RETURNING t_int2"
   :params [sql/int4-type]
   :result sql/result->one-row
   :row sql/row->one-column})

(def ?insert-t-int2
  {:table :types
   :columns [:t-int2]
   :returning [:id]
   :merge-fn (fn [row insert]
               (:id insert))})

(deftest test-update
  (with-open [db (test-db)]

    (let [id (sql/insert-one db ?insert-t-int2 {:t-int2 1})]

      (is (nil? (sql/insert-one! db :types {:t-int2 2})))

      (is (number? (sql/insert-one! db :types {:t-int2 2} :id)))

      (let [m (sql/insert-one! db :types {:t-int2 2} [:id])]
        (is (map? m))
        (is (= 2 (:t-int2 m)))
        (is (pos? (:id m))))

      (sql/update! db :types {:t-int2 3} "id = $1" [id])

      (let [val (sql/query db ?update-add-one [id])]
        (is (= 4 val))))))

(deftest test-hstore-strings
  (with-open [db (test-db)]
    (let [input {"hello" "world"}
          result (sql/insert-one! db :types {:t-hstore input} :t-hstore)]
      (is (= input result))
      (is (thrown? java.lang.IllegalArgumentException (sql/insert-one! db :types {:t-hstore {"invalid" :value}})))
      (is (thrown? java.lang.IllegalArgumentException (sql/insert-one! db :types {:t-hstore {:invalid "key"}})))
      )))

(deftest test-hstore-keywords
  (with-open [db (-> (test-db)
                     (sql/with-default-types sql/hstore-keyword-type))]
    (let [input {:hello "world"}
          result (sql/insert-one! db :types {:t-hstore input} :t-hstore)]
      (is (= input result))
      (is (thrown? java.lang.IllegalArgumentException (sql/insert-one! db :types {:t-hstore {"invalid" "key"}})))
      (is (thrown? java.lang.IllegalArgumentException (sql/insert-one! db :types {:t-hstore {:invalid :value}})))
      )))

(deftest test-prepared-insert
  (with-open [db (test-db)]
    (sql/with-transaction db
      (with-open [insert (sql/prepare-insert db {:table :num-types
                                                 :columns [:fint2 :fint4 :fint8]
                                                 :returning [:id]})]
        (let [result (insert {:fint2 2
                              :fint4 4
                              :fint8 8})]
          (is (pos? (:id result)))
          (is (= 2 (:fint2 result)))
          (is (= 4 (:fint4 result)))
          (is (= 8 (:fint8 result)))
          )))))

(deftest test-prepared-insert-no-return
  (with-open [db (test-db)]
    (sql/with-transaction db
      (with-open [insert (sql/prepare-insert db {:table :num-types
                                                 :columns [:fint2 :fint4 :fint8]})]
        (let [result (insert {:fint2 2
                              :fint4 4
                              :fint8 8})]

          ;; number of rows affected
          (is (= 1 result))
          )))))

(deftest test-prepared-insert-return-single-column
  (with-open [db (test-db)]
    (sql/with-transaction db
      (with-open [insert (sql/prepare-insert db {:table :num-types
                                                 :columns [:fint2 :fint4 :fint8]
                                                 :returning :id})]
        (let [result (insert {:fint2 2
                              :fint4 4
                              :fint8 8})]

          ;; value of :id
          (is (pos? result))
          )))))

;; FIXME: should also support :returning
(deftest test-prepared-update
  (with-open [db (test-db)]
    (sql/with-transaction db
      (with-open [update (sql/prepare-update db {:table :num-types
                                                 :columns [:fint2 :fint4]
                                                 :where "id = $1"
                                                 :where-params [:id]})]
        (let [rows-affected (update {:fint2 2
                                     :fint4 4
                                     :id 0})]
          ;; row with id should not exist and therefore not update
          (is (zero? rows-affected))
          )))))
