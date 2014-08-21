(require '[shadow.pgsql :as sql])

;; first you need to have database to run queries on
;; either use one you already have
;; or use the example.sql file in this directory
;; createdb shadow_example -E utf-8
;; psql -f example.sql shadow_example

;; change user and database if needed
;; :host defaults to "localhost"
;; :port defaults to 5432

(def db (sql/start {:user "zilence"
                    :database "shadow_example"}))

(sql/query db "SELECT * FROM examples")
;; => []

(sql/execute db "INSERT INTO projects (name, tags) VALUES ($1, $2)" "shadow-pgsql" #{"clojure" "postgresql"})
;; => 1


(sql/query db "SELECT COUNT(*) FROM projects")
;; => [{:count 1}]

;; default is to return query results as a vector of maps
;; lets change it for this query

(sql/query db {:sql "SELECT COUNT(*) FROM projects"
               :row sql/row->one-column
               :result sql/result->one-row})
;; => 1

;; http://www.postgresql.org/docs/9.3/static/functions-array.html
;; @> is postgres for "contains", both sides must be arrays

(sql/query db "SELECT * FROM projects WHERE tags @> ARRAY[$1]" "clojure")
(sql/query db "SELECT * FROM projects WHERE tags @> $1" ["clojure"])


;; => [{:id 1, :name "shadow-pgsql", :tags #<String[] [Ljava.lang.String;@7df059e6>}]
;; the default returns text[] as a String[]

;; can be overridden for this column only, use a set
(def db1 (sql/with-types db {:projects {:tags (sql/set-type sql/text-type)}}))

(sql/query db1 "SELECT * FROM projects WHERE tags @> ARRAY[$1]" "clojure")
;; => [{:id 1, :name "shadow-pgsql", :tags #{"clojure" "postgresql"}}]

;; you may alias columns, the type will be chosen correctly
(sql/query db1 "SELECT tags AS foo FROM projects WHERE tags @> ARRAY[$1]" "clojure")
;; => [{:foo #{"clojure" "postgresql"}}]

;; a type implementation is identified by it oid, so now all text[] are returned as sets
(def db2 (sql/with-default-types db (sql/set-type sql/text-type)))

(sql/query db2 "SELECT * FROM projects WHERE tags @> ARRAY[$1]" "clojure")

;; => [{:id 1, :name "shadow-pgsql", :tags #{"clojure" "postgresql"}}]

;; or vecs
(def db3 (sql/with-default-types db (sql/vec-type sql/text-type)))

(sql/query db3 "SELECT * FROM projects WHERE tags @> ARRAY[$1]" "clojure")
;; => [{:id 1, :name "shadow-pgsql", :tags ["clojure" "postgresql"]}]


;; (sql/stop db)

