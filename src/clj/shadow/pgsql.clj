(ns shadow.pgsql
  (:import [java.time OffsetDateTime]
           [shadow.pgsql
            Database
            DatabaseConfig
            DatabasePool
            DatabaseTask
            Connection
            SQL SQL$Builder
            TypeHandler
            TypeRegistry
            RowBuilder RowBuilder$Factory
            ResultBuilder ResultBuilder$Factory
            ColumnInfo
            Helpers
            PreparedSQL]
           [shadow.pgsql.types
            TypedArray
            ArrayReader
            Text
            Text$Conversion Types HStore$Handler HStore])

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
     (-with-connection ~db
                       (fn [^Connection con#]
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
  (fn [{:keys [column-naming] :as db}]
    (reify
      RowBuilder$Factory
      (create [_ columns]
        (let [column-names (->> columns
                                (map #(.-name ^ColumnInfo %))
                                (map #(from-sql-name column-naming %))
                                (into []))]
          (ClojureMapBuilder. column-names transform-fn))))))

(def row->map
  (row->map-transform identity))

(def ^{:doc "row is returned as the value of the first (only) column"}
  row->one-column
  Helpers/ONE_COLUMN)

(def ^{:doc "result is return as nil or row"}
  result->one-row
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
      (throw (ex-info "NULL not supported in sets" {})))
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
      (throw (ex-info "NULL not supported in vectors" {})))
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
  (vec-type Types/NUMERIC false))

(def text-type Types/TEXT)

(def text-vec-type
  (vec-type text-type))

(def timestamp-type
  Types/TIMESTAMP)

(def timestamp-vec-type
  (vec-type timestamp-type))

(def timestamptz-type
  Types/TIMESTAMPTZ)

(def timestamptz-vec-type
  (vec-type timestamptz-type))

(def hstore-string-type
  (HStore.
    (reify HStore$Handler
      (keyToString [_ key]
        (when-not (string? key)
          (throw (ex-info "not a valid key, must be a string" {:key key})))
        key)
      (valueToString [_ val]
        (when-not (string? val)
          (throw (ex-info "not a valid value, must be a string" {:value val})))
        val)
      (init [_ size]
        (transient {}))
      (add [_ m key value]
        (assoc! m key value))
      (complete [_ m]
        (persistent! m)))))

(def hstore-keyword-type
  (HStore.
    (reify HStore$Handler
      (keyToString [_ key]
        (when-not (and (keyword? key)
                       (nil? (namespace key)))
          (throw (ex-info "not a valid key, must be a keyword without namespace" {:key key})))
        (name key))
      (valueToString [_ val]
        (when-not (string? val)
          (throw (ex-info "not a valid value, must be a string" {:value val})))
        val)
      (init [_ size]
        (transient {}))
      (add [_ m key value]
        (assoc! m (keyword key) value))
      (complete [_ m]
        (persistent! m)))))

(defn- set-common-builder-args [db args ^SQL$Builder builder]
  (when-let [name (:name args)]
    (.withName builder name))

  (when-let [params (:params args)]
    (.withParamTypes builder params))

  (.withTypeRegistry builder (or (:types args)
                                 (:types db)
                                 TypeRegistry/DEFAULT)))

(defn- ^SQL$Builder set-row-builder [^SQL$Builder builder rbf db]
  (cond
    (instance? RowBuilder rbf)
    (.buildRowsWith builder ^RowBuilder rbf)
    (instance? RowBuilder$Factory rbf)
    (.withRowBuilder builder ^RowBuilder$Factory rbf)
    (fn? rbf)
    (.withRowBuilder builder ^RowBuilder$Factory (rbf db))
    :else
    (throw (ex-info "invalid :row builder, expected RowBuilder, RowBuilder$Factory or fn" {:given rbf}))))

(defn- ^SQL$Builder set-result-builder [^SQL$Builder builder rbf db]
  (cond
    (instance? ResultBuilder rbf)
    (.buildResultsWith builder ^ResultBuilder rbf)
    (instance? ResultBuilder$Factory rbf)
    (.withResultBuilder builder ^ResultBuilder$Factory rbf)
    (fn? rbf)
    (.withResultBuilder builder ^ResultBuilder$Factory (rbf db))
    :else
    (throw (ex-info "invalid :result builder, expected ResultBuilder, ResultBuilder$Factory or fn" {:given rbf}))))

(defn ^SQL as-query [db args]
  (if (instance? SQL args)
    args
    (let [[sql-string args] (cond
                              (string? args)
                              [args {}]
                              (map? args)
                              [(:sql args) args]
                              :else
                              (throw (ex-info "cannot build SQL from args" {:args args})))
          builder (SQL/query sql-string)]


      (set-row-builder builder (:row args row->map) db)
      (set-result-builder builder (:result args result->vec) db)

      (.create builder))))

(defn ^SQL as-statement [db args]
  (if (instance? SQL args)
    args
    (let [[sql-string args] (cond
                              (string? args)
                              [args {}]

                              (map? args)
                              [(:sql args) args]

                              :else
                              (throw (ex-info "cannot build SQL from args" {:args args})))
          builder (SQL/statement sql-string)]
      (set-common-builder-args db args builder)

      (.create builder)
      )))

(defn prepare-query [db stmt]
  (let [connection (get-connection db)
        query (as-query db stmt)
        ^PreparedSQL prep (.prepare connection query)]

    (reify
      java.lang.AutoCloseable
      (close [_]
        (.close prep))

      clojure.lang.IDeref
      (deref [_]
        prep)

      clojure.lang.IFn
      (invoke [_ p1]
        (.query prep [p1]))
      (invoke [_ p1 p2]
        (.query prep [p1 p2]))
      (invoke [_ p1 p2 p3]
        (.query prep [p1 p2 p3]))
      (invoke [_ p1 p2 p3 p4]
        (.query prep [p1 p2 p3 p4]))
      (invoke [_ p1 p2 p3 p4 p5]
        (.query prep [p1 p2 p3 p4 p5]))
      (invoke [_ p1 p2 p3 p4 p5 p6]
        (.query prep [p1 p2 p3 p4 p5 p6]))
      (invoke [_ p1 p2 p3 p4 p5 p6 p7]
        (.query prep [p1 p2 p3 p4 p5 p6 p7]))
      (invoke [_ p1 p2 p3 p4 p5 p6 p7 p8]
        (.query prep [p1 p2 p3 p4 p5 p6 p7 p8]))
      (invoke [_ p1 p2 p3 p4 p5 p6 p7 p8 p9]
        (.query prep [p1 p2 p3 p4 p5 p6 p7 p8 p9]))
      (invoke [_ p1 p2 p3 p4 p5 p6 p7 p8 p9 p10]
        (.query prep [p1 p2 p3 p4 p5 p6 p7 p8 p9 p10]))
      )))

(defn query
  ([db q]
   (query db q []))
  ([db query params]
   {:pre [(sequential? params)]}
   (let [query (as-query db query)]
     (-with-connection db (fn [^Connection con]
                            (.query con query params))))))

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
                          (map #(to-sql-name column-naming %))
                          (into []))

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

        query (-> (SQL/query sql)
                  ;; FIXME: find a way to specify the query name
                  (.withName (str "insert." table-name))
                  (.withParamTypes param-types)
                  (.withTypeRegistry types)
                  (set-row-builder row-builder db)
                  (set-result-builder result-builder db)
                  (.create))]

    (with-transaction db
      (with-open [prep (.prepare (get-connection db) query)]
        (if returning?
          (->> data
               (reduce (fn [result row]
                         (let [params (columns-fn row columns)
                               row-result (.query prep params)]
                           (conj! result (merge-fn row row-result))))
                       (transient []))
               (persistent!))
          ;; do not accumulate a result unless requested
          (doseq [row data]
            (.execute prep (columns-fn row columns)))
          )))))

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
        ^PreparedSQL prep (.prepare connection stmt)]

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

(defn execute
  ([db stmt]
   (execute db stmt []))
  ([db stmt params]
   {:pre [(sequential? params)]}
   (let [stmt (as-statement db stmt)]
     (-with-connection db (fn [^Connection con]
                            (-> (.execute con stmt params)
                                (.getRowsAffected)))))))

(defn update!
  "(sql/update! db :table {:column value, ...} \"id = $1\" id)
   this is also potentially dangerous since all columns are updated"
  ([db table data]
   (update! db table data "1=1" []))
  ([{:keys [table-naming column-naming ^TypeRegistry types] :as db} table data where params]
   {:pre [(sequential? params)]}
   (let [offset (count params)
         table-name (to-sql-name table-naming table)

         column-names (->> (keys data)
                           (map #(to-sql-name column-naming %)))

         sql (str "UPDATE "
                  (quoted table-name)
                  " SET "
                  (->> column-names
                       (map quoted)
                       (map-indexed (fn [idx col-name]
                                      (str col-name " = $" (+ idx offset 1))))
                       (str/join ", "))
                  " WHERE "
                  where)]
     (execute db
              {:sql sql
               ;; type hints for the columns since we know table/column
               ;; nil for remainder since we only know expected oid
               :params (->> column-names
                            (map #(.getTypeHandlerForColumn types table-name %))
                            (concat (repeat (count params) nil)))}
              (into params (vals data))))))

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
           types
           metric-registry
           metric-collector]
    :or {host "localhost"
         port 5432
         table-naming (DefaultNaming.)
         column-naming (DefaultNaming.)
         types TypeRegistry/DEFAULT}
    :as opts}]

  ;; FIXME: validate args
  (let [db-config (doto (DatabaseConfig. host port)
                    (.setUser user)
                    (.setDatabase database)
                    (.setMetricRegistry metric-registry))
        _ (when metric-collector
            (.setMetricCollector db-config metric-collector))
        db (.get db-config)
        pool (DatabasePool. db)]
    (-> (DB. pool table-naming column-naming types opts)
        ;; vec is a better default than arrays
        (with-default-types int2-vec-type
                            int4-vec-type
                            int8-vec-type
                            text-vec-type
                            numeric-vec-type
                            timestamp-vec-type
                            timestamptz-vec-type
                            ;; maybe hstore with keywords is a better default?
                            hstore-string-type))))

(defn stop [db]
  (.close ^java.lang.AutoCloseable db))

(defrecord DefQuery [stmt]
  clojure.lang.IFn
  (invoke [this db]
    (query db stmt []))
  (invoke [this db a1]
    (query db stmt [a1]))
  (invoke [this db a1 a2]
    (query db stmt [a1 a2]))
  (invoke [this db a1 a2 a3]
    (query db stmt [a1 a2 a3]))
  (invoke [this db a1 a2 a3 a4]
    (query db stmt [a1 a2 a3 a4]))
  (invoke [this db a1 a2 a3 a4 a5]
    (query db stmt [a1 a2 a3 a4 a5]))
  (invoke [this db a1 a2 a3 a4 a5 a6]
    (query db stmt [a1 a2 a3 a4 a5 a6]))
  (invoke [this db a1 a2 a3 a4 a5 a6 a7]
    (query db stmt [a1 a2 a3 a4 a5 a6 a7]))
  (invoke [this db a1 a2 a3 a4 a5 a6 a7 a8]
    (query db stmt [a1 a2 a3 a4 a5 a6 a7 a8]))
  (invoke [this db a1 a2 a3 a4 a5 a6 a7 a8 a9]
    (throw (ex-info "I'M LAZY! need more args" {}))))

(defmacro defquery [name sql & kv]
  (let [sql (-> sql (str/replace #"\s+" " ") (.trim))
        query-name (str *ns* "/" name)]
    `(def ~name (->DefQuery (array-map :name ~query-name :sql ~sql ~@kv)))))
