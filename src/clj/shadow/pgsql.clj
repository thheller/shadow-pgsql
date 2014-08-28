(ns shadow.pgsql
  (:import [java.time OffsetDateTime]
           [shadow.pgsql
            Database
            DatabasePool
            Connection
            Query
            Statement
            TypeHandler
            TypeRegistry
            RowBuilder
            ResultBuilder
            ColumnInfo
            Helpers
            SimpleQuery PreparedStatement PreparedQuery DatabaseTask DatabaseConfig]
           [shadow.pgsql.types
            TypedArray
            ArrayReader
            Text
            Text$Conversion Types])

  (:require [clojure.string :as str]
            [clojure.edn :as edn]
            [shadow.pgsql :as sql]))

(set! *warn-on-reflection* true)

(defprotocol Naming
  (to-sql-name [this value])
  (from-sql-name [this string]))

(defn with-dashes [^String x]
  (.replaceAll x "_" "-"))

(defn with-underscores [^String x]
  (.replaceAll x "-" "_"))

(deftype DefaultNaming []
  Naming
  (to-sql-name [_ kw]
    (with-underscores (name kw)))
  (from-sql-name [_ sv]
    (keyword (with-dashes sv))))

(def ^{:private true
       :dynamic true}
  *open-connections* {})

(defprotocol ConnectionScope
  (-with-connection [_ task-fn])
  (-get-connection [_]))

(defrecord DB [^DatabasePool pool table-naming column-naming types opts]
  java.lang.AutoCloseable
  (close [_]
    (.close ^DatabasePool pool))

  ConnectionScope
  (-get-connection [_]
    (get *open-connections* (.getPoolId pool)))
  (-with-connection [_ task-fn]
    (let [db-id (.getPoolId pool)]
      (if-let [con (get *open-connections* db-id)]
        (task-fn con)
        (.withConnection pool (reify
                                DatabaseTask
                                (withConnection [_ con]
                                  (binding [*open-connections* (assoc *open-connections* db-id con)]
                                    (task-fn con)))))))))

(defmacro with-connection [db & body]
  `(-with-connection ~db (fn [con#] ~@body)))

(defmacro with-transaction [db & body]
  `(let [body-fn# (fn [] ~@body)]
     (-with-connection ~db (fn [^Connection con#]
                             (if (.isInTransaction con#)
                               (body-fn#)
                               (do (.begin con#)
                                   (try
                                     (let [result# (body-fn#)]
                                       (.commit con#)
                                       result#)
                                     (catch Throwable t#
                                       (.rollback con#)
                                       (throw t#)
                                       ))))))))

(defn ^Connection get-connection [db]
  (if-let [con (-get-connection db)]
    con
    (throw (IllegalStateException. "no open connection found, use within with-connection/with-transaction"))
    ))

(defn- quoted [^String s]
  (when (.contains s "\"")
    (throw (ex-info "names cannot contain quotes" {:s s})))
  (str \" s \"))

(deftype ClojureMapBuilder [column-names transform-fn]
  RowBuilder
  (init [_]
    (transient {}))

  (add [_ state column-info col-index col-value]
    ;; FIXME: do I want {:some-column nil} in my map?
    (if (nil? col-value)
      state
      (assoc! state (nth column-names col-index) col-value)))

  (complete [_ state]
    (transform-fn (persistent! state))))

(def result->vec
  (reify
    ResultBuilder
    (init [_]
      (transient []))
    (add [_ state row]
      (conj! state row))
    (complete [_ state]
      (persistent! state))))

(defn result->map
  "{:sql \"SELECT * FROM something\"
    :result (sql/result->map :id)}

   will return {id1 row1, id2 row2, ...}

   {:sql \"SELECT key, value FROM something\"
    :result (sql/result->map :key :value)}

   returns {key1 value1, key2, value2, ...}"
  ([key-fn]
    (result->map key-fn identity))
  ([key-fn value-fn]
    (result->map key-fn value-fn {}))
  ([key-fn value-fn init]
   (reify
     ResultBuilder
     (init [_]
       (transient init))
     (add [_ state row]
       (assoc! state (key-fn row) (value-fn row)))
     (complete [_ state]
       (persistent! state)))))

(defn row->map-transform
  "returns row as map after calling (transform-fn map)"
  [transform-fn]
  (fn [{:keys [column-naming] :as db} columns]
    (let [column-names (->> columns
                            (map #(.-name ^ColumnInfo %))
                            (map #(from-sql-name column-naming %))
                            (into []))]
      (ClojureMapBuilder. column-names transform-fn))))

(def row->map
  (row->map-transform identity))

(defn row->one-column
  "row is returned as the value of the first (only) column"
  [db columns]
  Helpers/ONE_COLUMN)

(defn result->one-row
  "result is return as nil or row"
  [db columns]
  Helpers/ONE_ROW)

(defn now []
  (OffsetDateTime/now))

(def ^{:doc "stores keywords without leading \":\""}
  legacy-keyword-type
  (Text.
    (reify Text$Conversion
      (encode [_ param]
        (when-not (keyword? param)
          (throw (IllegalArgumentException. (format "not a keyword: %s" (pr-str param)))))
        (.substring (str param) 1))
      (decode [_ ^String s]
        (when (= \: (.charAt s 0))
          (throw (ex-info "keyword with a :" {:s s})))

        (let [idx (.indexOf s "/")]
          (if (not= -1 idx)
            (keyword (.substring s 0 idx)
                     (.substring s idx))
            (keyword s)))))))

(def ^{:doc "stores keywords as-is"}
  keyword-type
  (Text.
    (reify Text$Conversion
      (encode [_ param]
        (when-not (keyword? param)
          (throw (IllegalArgumentException. (format "not a keyword: %s" (pr-str param)))))
        (str param))
      (decode [_ ^String s]
        (when-not (= \: (.charAt s 0))
          (throw (ex-info "not a keyword" {:s s})))

        (let [idx (.indexOf s "/")]
          (if (not= -1 idx)
            (keyword (.substring s 1 idx)
                     (.substring s idx))
            (keyword (.substring s 1))))))))

(defn edn-type
  ([]
   (edn-type {}))
  ([edn-opts]
   (Text.
     (reify Text$Conversion
       (encode [_ param]
         (pr-str param))
       (decode [_ ^String s]
         (edn/read-string edn-opts s))
       ))))

(def ^:private clojure-set-reader
  (reify
    ArrayReader
    (init [_ size]
      (transient #{}))
    (add [_ state index object]
      (conj! state object))
    (addNull [_ state index]
      ;; FIXME: probably to harsh to throw
      (throw (ex-info "NULL not supported in sets")))
    (complete [_ state]
      (persistent! state))))

(defn set-type
  ([item-type]
   (set-type item-type true))
  ([item-type requires-quoting]
   (TypedArray. item-type clojure-set-reader requires-quoting)))

(def keyword-set-type
  (set-type keyword-type))

(def ^:private clojure-vec-reader
  (reify
    ArrayReader
    (init [_ size]
      (transient []))
    (add [_ state index object]
      (conj! state object))
    (addNull [_ state index]
      ;; FIXME: probably to harsh to throw
      (throw (ex-info "NULL not supported in vectors")))
    (complete [_ state]
      (persistent! state))))

(defn vec-type
  ([item-type]
   (vec-type item-type true))
  ([item-type requires-quoting]
   (TypedArray. item-type clojure-vec-reader requires-quoting)))

(def keyword-vec-type
  (vec-type keyword-type))

(def int2-vec-type
  (vec-type Types/INT2 false))

(def int2-type Types/INT2)
(def short-type Types/INT2)

(def int-type Types/INT4)
(def int4-type Types/INT4)

(def int8-type Types/INT8)
(def long-type Types/INT8)

(def int4-vec-type
  (vec-type Types/INT4 false))

(def int-vec-type
  (vec-type Types/INT4 false))

(def int-set-type
  (set-type Types/INT4 false))

(def int4-set-type
  (set-type Types/INT4 false))

(def int8-vec-type
  (vec-type Types/INT8 false))

(def long-vec-type
  (vec-type Types/INT8 false))

(def int8-set-type
  (set-type Types/INT8 false))

(def long-set-type
  (set-type Types/INT8 false))

(def numeric-vec-type
  (vec-type Types/NUMERIC))

(def text-type Types/TEXT)

(def text-vec-type
  (vec-type text-type))

(def timestamp-type
  Types/TIMESTAMP)

(def timestamptz-type
  Types/TIMESTAMPTZ)

(deftype ClojureStatement [db sql params types]
  Statement
  (getSQLString [_]
    sql)

  (getParameterTypes [_]
    params)

  (getTypeRegistry [_]
    types))

(deftype ClojureQuery [db sql params types result-builder row-builder]
  Query
  (getSQLString [this]
    sql)

  (getParameterTypes [this]
    params)

  (getTypeRegistry [this]
    types)

  (createResultBuilder [this columns]
    (if (fn? result-builder)
      (result-builder db columns)
      result-builder))

  (createRowBuilder [this columns]
    (if (fn? row-builder)
      (row-builder db columns)
      row-builder)))

(defn ^Query as-query [db args]
  (cond
    (string? args)
    (ClojureQuery. db args [] (:types db) result->vec row->map)

    (map? args)
    (let [{:keys [sql params types result row]} args]
      (ClojureQuery. db
                     sql
                     (or params [])
                     (or types
                         (:types db)
                         TypeRegistry/DEFAULT)
                     (or result result->vec)
                     (or row row->map)))

    (instance? Query args)
    args

    :else
    (throw (ex-info "cannot build query from args" {:args args}))))

(defn ^Statement as-statement [db args]
  (cond
    (string? args)
    (ClojureStatement. db args [] (:types db))

    (map? args)
    (let [{:keys [sql params types]} args]
      (ClojureStatement. db
                         sql
                         (or params [])
                         (or types
                             (:types db)
                             TypeRegistry/DEFAULT)))

    (instance? Statement args)
    args
    ))

(defn prepare-query [db stmt]
  (let [connection (get-connection db)
        query (as-query db stmt)
        ^PreparedQuery prep (.prepareQuery connection query)]

    (reify
      java.lang.AutoCloseable
      (close [_]
        (.close prep))

      clojure.lang.IDeref
      (deref [_]
        prep)

      clojure.lang.IFn
      (invoke [_ p1]
        (.execute prep [p1]))
      (invoke [_ p1 p2]
        (.execute prep [p1 p2]))
      (invoke [_ p1 p2 p3]
        (.execute prep [p1 p2 p3]))
      (invoke [_ p1 p2 p3 p4]
        (.execute prep [p1 p2 p3 p4]))
      (invoke [_ p1 p2 p3 p4 p5]
        (.execute prep [p1 p2 p3 p4 p5]))
      (invoke [_ p1 p2 p3 p4 p5 p6]
        (.execute prep [p1 p2 p3 p4 p5 p6]))
      (invoke [_ p1 p2 p3 p4 p5 p6 p7]
        (.execute prep [p1 p2 p3 p4 p5 p6 p7]))
      (invoke [_ p1 p2 p3 p4 p5 p6 p7 p8]
        (.execute prep [p1 p2 p3 p4 p5 p6 p7 p8]))
      (invoke [_ p1 p2 p3 p4 p5 p6 p7 p8 p9]
        (.execute prep [p1 p2 p3 p4 p5 p6 p7 p8 p9]))
      (invoke [_ p1 p2 p3 p4 p5 p6 p7 p8 p9 p10]
        (.execute prep [p1 p2 p3 p4 p5 p6 p7 p8 p9 p10]))
      )))

(defn query
  [db query & params]
  (let [query (as-query db query)
        params (into [] params)]

    (-with-connection db (fn [^Connection con]
                           (.executeQuery con query params)))))

(defn insert
  [db
   {:keys [table columns columns-fn returning merge-fn]
    row-builder :row
    result-builder :result
    :as args
    :or {columns-fn (fn [row columns]
                      (mapv #(get row %) columns))
         merge-fn merge}}
   data]
  (when-not (and (vector? columns)
                 (seq columns))
    (throw (ex-info "need a vector of columns" args)))

  (let [{:keys [table-naming column-naming ^TypeRegistry types]} db
        table-name (to-sql-name table-naming table)

        result-builder (or result-builder sql/result->one-row)

        row-builder (or row-builder sql/row->map)

        column-names (->> columns
                          (map #(to-sql-name column-naming %)))

        param-types (->> column-names
                         (map #(.getTypeHandlerForColumn types table-name %))
                         (into []))

        returning? (and (not (nil? returning))
                        (not (empty? returning)))

        returning-names (if returning?
                          (->> returning
                               (map #(to-sql-name column-naming %))
                               (map quoted))
                          ["1"])

        sql (str "INSERT INTO "
                 table-name
                 " ("
                 (->> column-names
                      (map quoted)
                      (str/join ","))
                 ") VALUES ("
                 (->> column-names
                      (count)
                      (range)
                      (map inc) ;; 1-idx
                      (map #(str "$" %))
                      (str/join ","))
                 ") RETURNING "
                 (str/join ", " returning-names)) ;; already quoted if needed

        query (ClojureQuery. db sql param-types types result-builder row-builder)]

    (with-transaction db
      (with-open [prep (.prepareQuery (get-connection db) query)]
        (->> data
             (reduce (fn [result row]
                       (let [params (columns-fn row columns)
                             row-result (.execute prep params)]
                         (if returning?
                           (conj! result (merge-fn row row-result))
                           (conj! result row)
                           )))
                     (transient []))
             (persistent!))))))

(defn insert-one [db stmt data]
  (first (insert db stmt [data])))

;; FIXME: this might be too confusing
(defn insert-one!
  "shorthand for insert-one, but potential DANGER!

   don't use with maps you got from an untrusted source
   will insert all values contained in the data map

   if you expect
   {:user \"name\" :password \"me\"}
   but the user sends you
   {:user \"name\" :password \"me\" :admin true}
   you might run into trouble

   use insert-one and specify {:columns [:user :password]} if you want to be safe.

   (sql/insert-one! db :table data) returns the inserted data
   (sql/insert-one! db :table data :id) returns the id of the inserted row
   (sql/insert-one! db :table data [:id]) returns the data merged with the resulting map {:id ...}
   "
  ([db table data]
   (insert-one! db table data nil))
  ([db table data return-column]
   (first (insert db
                  {:table table
                   :columns (into [] (keys data))
                   :merge-fn (fn [row insert]
                               (cond
                                 (nil? return-column)
                                 row

                                 (keyword? return-column)
                                 (get insert return-column)

                                 (vector? return-column)
                                 (merge row insert)))
                   :returning (cond
                                (nil? return-column)
                                nil

                                (keyword? return-column)
                                [return-column]

                                (vector? return-column)
                                return-column

                                :else
                                (throw (ex-info "invalid return value" {:table table
                                                                        :data data
                                                                        :return return-column})))}
                  [data]))))

(defn prepare [db stmt]
  (let [connection (get-connection db)
        stmt (as-statement db stmt)
        ^PreparedStatement prep (.prepare connection stmt)]

    (reify
      java.lang.AutoCloseable
      (close [_]
        (.close prep))

      clojure.lang.IDeref
      (deref [_]
        prep)

      clojure.lang.IFn
      (invoke [_ p1]
        (.execute prep [p1]))
      (invoke [_ p1 p2]
        (.execute prep [p1 p2]))
      (invoke [_ p1 p2 p3]
        (.execute prep [p1 p2 p3]))
      (invoke [_ p1 p2 p3 p4]
        (.execute prep [p1 p2 p3 p4]))
      (invoke [_ p1 p2 p3 p4 p5]
        (.execute prep [p1 p2 p3 p4 p5]))
      (invoke [_ p1 p2 p3 p4 p5 p6]
        (.execute prep [p1 p2 p3 p4 p5 p6]))
      (invoke [_ p1 p2 p3 p4 p5 p6 p7]
        (.execute prep [p1 p2 p3 p4 p5 p6 p7]))
      (invoke [_ p1 p2 p3 p4 p5 p6 p7 p8]
        (.execute prep [p1 p2 p3 p4 p5 p6 p7 p8]))
      (invoke [_ p1 p2 p3 p4 p5 p6 p7 p8 p9]
        (.execute prep [p1 p2 p3 p4 p5 p6 p7 p8 p9]))
      (invoke [_ p1 p2 p3 p4 p5 p6 p7 p8 p9 p10]
        (.execute prep [p1 p2 p3 p4 p5 p6 p7 p8 p9 p10]))
      )))

(defn execute [db stmt & params]
  (let [stmt (as-statement db stmt)
        params (into [] params)]
    (-with-connection db (fn [^Connection con]
                           (-> (.execute con stmt params)
                               (.getRowsAffected))))))

(defn update!
  "(sql/update! db :table {:column value, ...} \"id = $1\" id)
   this is also potentially dangerous since all columns are updated"
  [{:keys [table-naming column-naming] :as db} table data where & params]
  (let [offset (count params)
        sql (str "UPDATE "
                 (-> (to-sql-name table-naming table) quoted)
                 " SET "
                 (->> (keys data)
                      (map #(to-sql-name column-naming %))
                      (map quoted)
                      (map-indexed (fn [idx col-name]
                                     (str col-name " = $" (+ idx offset 1))))
                      (str/join ","))
                 " WHERE "
                 where)]
    (apply execute db sql (concat params (vals data)))))

(defn build-types
  ([type-map table-naming column-naming]
   (build-types TypeRegistry/DEFAULT type-map table-naming column-naming))
  ([^TypeRegistry type-registry type-map table-naming column-naming]
   (let [new-registry (.copy type-registry)]

     (doseq [[table-name columns] type-map
             :let [table-name (to-sql-name table-naming table-name)]
             [column-name handler] columns
             :let [column-name (to-sql-name column-naming column-name)]]
       (.registerColumnHandler new-registry table-name column-name handler))

     (.build new-registry))))

(defn with-types
  "copy db instance with custom type handlers {table-name {column-name type-handler}}
   types can also be declared per query"
  [{:keys [table-naming column-naming] :as db} type-map]
  (let [^TypeRegistry type-registry (:types db)]
    (assoc db :types (build-types type-registry type-map table-naming column-naming))))

(defn with-default-types [db & handlers]
  (let [^TypeRegistry type-registry (:types db)
        new-registry (.copy type-registry)]

    (doseq [handler handlers]
      (.registerTypeHandler new-registry handler))

    (assoc db :types (.build new-registry))))

(defn start
  [{:keys [host
           port
           user
           database
           pool
           table-naming
           column-naming
           types]
    :or {host "localhost"
         port 5432
         table-naming (DefaultNaming.)
         column-naming (DefaultNaming.)
         types TypeRegistry/DEFAULT}
    :as opts}]

  ;; FIXME: validate args
  (let [db-config (doto (DatabaseConfig. host port)
                    (.setUser user)
                    (.setDatabase database))
        db (.get db-config)
        pool (DatabasePool. db)]
    (-> (DB. pool table-naming column-naming types opts)
        ;; vec is a better default than arrays
        (with-default-types int2-vec-type
                            int4-vec-type
                            int8-vec-type
                            text-vec-type
                            numeric-vec-type))))

(defn stop [db]
  (.close ^java.lang.AutoCloseable db))

(defrecord DefQuery [stmt]
  clojure.lang.IFn
  (invoke [this db]
    (query db stmt))
  (invoke [this db a1]
    (query db stmt a1))
  (invoke [this db a1 a2]
    (query db stmt a1 a2))
  (invoke [this db a1 a2 a3]
    (query db stmt a1 a2 a3))
  (invoke [this db a1 a2 a3 a4]
    (query db stmt a1 a2 a3 a4))
  (invoke [this db a1 a2 a3 a4 a5]
    (query db stmt a1 a2 a3 a4 a5))
  (invoke [this db a1 a2 a3 a4 a5 a6]
    (query db stmt a1 a2 a3 a4 a5 a6))
  (invoke [this db a1 a2 a3 a4 a5 a6 a7]
    (query db stmt a1 a2 a3 a4 a5 a6 a7))
  (invoke [this db a1 a2 a3 a4 a5 a6 a7 a8]
    (query db stmt a1 a2 a3 a4 a5 a6 a7 a8))
  (invoke [this db a1 a2 a3 a4 a5 a6 a7 a8 a9]
    (throw (ex-info "I'M LAZY! need more args" {}))))

(defmacro defquery [name sql & kv]
  (let [sql (-> sql (str/replace #"\s+" " ") (.trim))]
    `(def ~name (->DefQuery (array-map :sql ~sql ~@kv)))))
