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
            Text])

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

(def ^{:private true
       :dynamic true}
  *open-connections* {})

(defrecord DB [pool table-naming column-naming types opts]
  java.lang.AutoCloseable
  (close [_]
    (.close ^DatabasePool pool)))

(defn with-connection* [{:keys [^DatabasePool pool] :as db} handler]
  (let [db-id (.getPoolId pool)]
    (if-let [con (get *open-connections* db-id)]
      (handler con) ;; reuse open connection
      (let [con (.borrowObject pool)]
        (try
          (let [result (binding [*open-connections* (assoc *open-connections*
                                                      db-id con)]
                         (handler con))]
            (.returnObject pool con)
            result)
          (catch Throwable t
            (.invalidateObject pool con)
            (throw t)))))))

(defmacro with-connection [[con db] & body]
  `(with-connection* ~db
     (fn [~con]
       ~@body)))

(defmacro with-transaction [[con db] & body]
  `(with-connection* ~db
     (fn [~con]
       (.begin ~con)
       (try
         (let [result# (do ~@body)]
           (.commit ~con)
           result#) 
         (catch Throwable t#
           (.rollback ~con)
           (throw t#)
           )))))

(defn- quoted [^String s]
  (when-not (= -1 (.indexOf s "\""))
    (throw (ex-info "names cannot contain quotes" {:s s})))
  (str \" s \"))

(deftype ClojureResultBuilder [init-fn reduce-fn complete-fn]
  ResultBuilder
  (init [_]
    (init-fn))
  (add [_ state row]
    (reduce-fn state row))
  (complete [_ state]
    (complete-fn state)))

(deftype ClojureMapBuilder [column-naming]
  RowBuilder
  (init [_] (transient {}))
  (add [_ state column-info col-index col-value]
    ;; FIXME: do I want {:some-column nil} in my map?
    (if (nil? col-value)
      state
      (assoc! state (nth column-naming col-index) col-value)))
  (complete [_ state]
    (persistent! state)))

(defn result->vec [db columns]
  (ClojureResultBuilder. #(transient []) conj! persistent!))

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
  Helpers/SINGLE_COLUMN)

(defn result->one-row
  "result is return as nil or row"
  [db columns]
  Helpers/SINGLE_ROW)

(def keyword-type
  (Text. 
   (reify shadow.pgsql.types.Text$Conversion
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

(defn query [db query & params]
  (let [query (as-query db query)
        params (into [] params)]
    (with-connection [^Connection con db]
      (.executeQuery con query params))))

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
        table-name (->> (:table args)
                        (to-sql-name table-naming))

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
    (with-transaction [^Connection con db]
      (with-open [prep (.prepareQuery con query)]
        (->> data
             (reduce (fn [result row]
                       (let [params (columns-fn row columns)
                             row-result (.execute prep params)]
                         (conj! result (merge-fn row row-result))))
                     (transient []))
             (persistent!))
        ))))

(defn execute [db stmt & params]
  (let [stmt (as-statement db stmt)
        params (into [] params)]
    (with-connection [^Connection con db]
      (-> (.execute con stmt params)
          (.getRowsAffected)))))

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
               [column-name handler] columns
               :let [table-name (to-sql-name table-naming table-name)
                     column-name (to-sql-name column-naming column-name)]]
         (.registerColumnHandler new-registry table-name column-name handler))

       (.build new-registry))))

(defn with-types
  "copy db instance with custom type handlers {table-name {column-name type-handler}}
   types can also be declared per query"
  [{:keys [table-naming column-naming] :as db} type-map]
  (let [^TypeRegistry type-registry (:types db)]
    (assoc db :types (build-types type-registry type-map table-naming column-naming))))

(defn stop [db]
  (.close ^java.lang.AutoCloseable db))
