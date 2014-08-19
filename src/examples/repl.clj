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

(pprint (sql/query db "SELECT * FROM examples"))

;; => []

;; (sql/stop db)

