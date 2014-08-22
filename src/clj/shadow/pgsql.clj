(ns shadow.pgsql
  (:import [shadow.pgsql
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
            SimpleQuery]
           [shadow.pgsql.types
            TypedArray
            ArrayReader
            Text
            Text$Conversion Types])

  (:require [clojure.string :as str]))


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

(defprotocol WithConnection
  (-with-connection [_ task-fn]))

(defrecord DB [pool table-naming column-naming types opts]
  java.lang.AutoCloseable
  (close [_]
    (.close ^DatabasePool pool))

  WithConnection
  (-with-connection [_ task-fn]
    (let [con (.borrowObject pool)]
      (try
        (let [result (task-fn con)]
          (.returnObject pool con)
          result)
        (catch Throwable t
          (.invalidateObject pool con)
          (throw t))))))

(defrecord ConnectedDB [connection pool table-naming column-naming types opts]
  java.lang.AutoCloseable
  (close [_]
    (.returnObject ^DatabasePool pool ^Connection connection))
  WithConnection
  (-with-connection [_ task-fn]
    ;; FIXME: invalidate con?
    (task-fn connection)))

(defn db->connected-db [{:keys [pool table-naming column-naming types opts] :as db}]
  (when (instance? ConnectedDB db)
    (throw (ex-info "already connected" {:db db})))

  (ConnectedDB. (.borrowObject pool)
                pool
                table-naming
                column-naming
                types
                opts))

(defmacro with-connection [[con db] & body]
  `(with-open [~con (db->connected-db ~db)]
     ~@body))

(defmacro with-transaction [[con db] & body]
  `(with-open [~con (db->connected-db ~db)]
     (let [con# (:connection ~con)]
       (.begin con#)
       (try
         (let [result# (do ~@body)]
           (.commit con#)
           result#)
         (catch Throwable t#
           (.rollback con#)
           (throw t#)
           )))))

(defn- quoted [^String s]
  (when-not (= -1 (.indexOf s "\""))
    (throw (ex-info "names cannot contain quotes" {:s s})))
  (str \" s \"))

(deftype ClojureMapBuilder [column-names]
  RowBuilder
  (init [_]
    (transient {}))

  (add [_ state column-info col-index col-value]
    ;; FIXME: do I want {:some-column nil} in my map?
    (if (nil? col-value)
      state
      (assoc! state (nth column-names col-index) col-value)))

  (complete [_ state]
    (persistent! state)))

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
  "{:result (sql/result->map :id)}

   SELECT * FROM something

   will return {id1 row1, id2 row2, ...}"
  [key-fn]
  (reify
    ResultBuilder
    (init [_]
      (transient {}))
    (add [_ state row]
      (assoc! state (key-fn row) row))
    (complete [_ state]
      (persistent! state))))

(defn row->map
  [{:keys [column-naming] :as db} columns]
  (let [lookup-array (->> columns
                          (map #(.-name ^ColumnInfo %))
                          (map #(from-sql-name column-naming %))
                          (into-array))]
    (ClojureMapBuilder. lookup-array)))

(defn row->one-column
  "row is returned as the value of the first (only) column"
  [db columns]
  Helpers/ONE_COLUMN)

(defn result->one-row
  "result is return as nil or row"
  [db columns]
  Helpers/ONE_ROW)

(def ^{:doc "stores keywords as text without the : so :hello/world is \"hello/world\" "}
  keyword-type
  (Text.
    (reify Text$Conversion
      (encode [_ param]
        (when-not (keyword? param)
          (throw (IllegalArgumentException. (format "not a keyword: %s" (pr-str param)))))
        (.substring (str param) 1))
      (decode [_ ^String s]
        (let [idx (.indexOf s "/")]
          (if (not= -1 idx)
            (keyword (.substring s 0 idx)
                     (.substring s idx))
            (keyword s)))))))

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
  [item-type]
  (TypedArray. item-type clojure-set-reader))

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
  [item-type]
  (TypedArray. item-type clojure-vec-reader))

(def text-type Types/TEXT)

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

(defn as-query [db args]
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

(defn as-statement [db args]
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
    :as args}
   data]
  (when-not (and (vector? columns)
                 (seq columns))
    (throw (ex-info "need a vector of columns" args)))

  (let [{:keys [table-naming column-naming types]} db
        table-name (to-sql-name table-naming table)

        column-names (->> columns
                          (map #(to-sql-name column-naming %)))

        param-types (->> column-names
                         (map #(.getTypeHandlerForColumn types table-name %))
                         (into []))

        returning-names (->> returning
                             (map #(to-sql-name column-naming %))
                             (map quoted))

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
    (with-transaction [con-db db]
      (-with-connection con-db
                        (fn [^Connection con]
                          (with-open [prep (.prepareQuery con query)]
                            (->> data
                                 (reduce (fn [result row]
                                           (let [params (columns-fn row columns)
                                                 row-result (.execute prep params)]
                                             (conj! result (merge-fn row row-result))))
                                         (transient []))
                                 (persistent!))
                            ))))))

(defn prepare [{:keys [connection] :as db} stmt]
  (when-not (instance? ConnectedDB db)
    (throw (ex-info "not a connected db, please use with-connection" {:db db})))

  (let [stmt (as-statement db stmt)
        prep (.prepare connection stmt)]

    (reify
      java.lang.AutoCloseable
      (close [_]
        (.close prep))

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
  (let [db (Database/setup host port user database)
        pool (DatabasePool. db)]
    (DB. pool table-naming column-naming types opts)
    ))

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

(defn stop [db]
  (.close ^java.lang.AutoCloseable db))
