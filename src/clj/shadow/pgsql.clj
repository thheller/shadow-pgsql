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
            Text$Conversion Types HStore$Handler HStore]
           (clojure.lang PersistentArrayMap))

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
    (assoc! state (nth column-names col-index) col-value))

  (complete [_ state]
    (transform-fn (persistent! state))))

(deftype ClojureArrayMapBuilder [size column-names transform-fn]
  RowBuilder
  (init [_]
    (object-array size))

  (add [_ state column-info col-index col-value]
    (let [idx (* 2 col-index)]
      (aset ^objects state idx (nth column-names col-index))
      (aset ^objects state (inc idx) col-value)
      state))

  (complete [_ state]
    (transform-fn (PersistentArrayMap. ^objects state))
    ))

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
        (let [count (alength columns)
              column-names (->> columns
                                (map #(.-name ^ColumnInfo %))
                                (map #(from-sql-name column-naming %))
                                (into []))]
          (if (<= count 8) ;; PersistentArrayMap.HASHTABLE_THRESHOLD/2 (not accessible from here)
            ;; array map builder only allocates one array per row and later wraps that into a PersistentArrayMap
            (ClojureArrayMapBuilder. (* count 2) column-names transform-fn)
            ;; uses transients but that will create/copy with each new pair, lots of garbage
            (ClojureMapBuilder. column-names transform-fn)))))))

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

(def numeric-type
  Types/NUMERIC)

(def numeric-vec-type
  (vec-type Types/NUMERIC false))

(def text-type Types/TEXT)

(def text-vec-type
  (vec-type text-type))

(def timestamp-type
  Types/TIMESTAMP)

(def date-type
  Types/DATE)

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

(defn- ^SQL$Builder set-common-builder-args [^SQL$Builder builder db args]
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
                              (throw (ex-info "cannot build SQL from args" {:args args})))]

      (-> (SQL/query sql-string)
          (set-common-builder-args db args)
          (set-row-builder (:row args row->map) db)
          (set-result-builder (:result args result->vec) db)
          (.create)))))

(defn ^SQL as-statement [db args]
  (if (instance? SQL args)
    args
    (let [[sql-string args] (cond
                              (string? args)
                              [args {}]

                              (map? args)
                              [(:sql args) args]

                              :else
                              (throw (ex-info "cannot build SQL from args" {:args args})))]

      (-> (SQL/statement sql-string)
          (set-common-builder-args db args)
          (.create)))))

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

(defn create-insert-sql
  [{:keys [^TypeRegistry types column-naming table-naming] :as db}
   {:keys [table columns returning] :as spec}]
  (when-not (and (vector? columns)
                 (seq columns)
                 (not (nil? table)))
    (throw (ex-info "need a vector of :columns and :table name" {:spec spec})))

  (let [table-name (sql/to-sql-name table-naming table)

        result-builder (or (:result-builder spec)
                           result->one-row)

        row-builder (or (:row-builder spec)
                        row->map)

        column-names (->> columns
                          (map #(to-sql-name column-naming %))
                          (into []))

        columns-fn (or (:columns-fn spec)
                       (fn [row]
                         (mapv #(get row %) columns)))

        param-types (->> column-names
                         (map #(.getTypeHandlerForColumn types table-name %))
                         (into []))

        returning? (or (keyword? returning)
                       (seq returning))

        returning-names (when returning?
                          (->> (if (keyword? returning)
                                 [returning]
                                 returning)
                               (map #(to-sql-name column-naming %))
                               (map quoted)))

        sql (str "INSERT INTO "
                 (sql/quoted table-name)
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
                 ")"
                 (when returning?
                   (str " RETURNING " (str/join ", " returning-names))))

        merge-fn (or (:merge-fn spec)
                     (cond
                       (not returning?)
                       nil

                       (keyword? returning)
                       (fn [row insert]
                         (get insert returning))

                       (vector? returning)
                       merge))

        sql (if returning?
              (-> (SQL/query sql)
                  (sql/set-row-builder row-builder db)
                  (sql/set-result-builder result-builder db))
              (SQL/statement sql))

        sql (-> sql
                (.withName (or (:name spec)
                               (str "insert." table-name)))
                (.withParamTypes param-types)
                (.withTypeRegistry types)
                (.create))]

    {:sql sql
     :columns columns
     :returning? returning?
     :columns-fn columns-fn
     :merge-fn merge-fn}
    ))

(defn insert
  [db spec data]
  (let [{:keys [sql returning? columns-fn merge-fn]} (create-insert-sql db spec)]
    (with-transaction db
      (with-open [prep (.prepare (get-connection db) sql)]
        (if returning?
          (->> data
               (reduce (fn [result row]
                         (let [params (columns-fn row)
                               row-result (.query prep params)]
                           (conj! result (merge-fn row row-result))))
                       (transient []))
               (persistent!))
          ;; do not accumulate a result unless requested
          (doseq [row data]
            (.execute prep (columns-fn row)))
          )))))

(defn insert-one [db stmt data]
  (->> [data]
       (insert db stmt)
       (first)))

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
   (->> [data]
        (insert
          db
          {:table table
           :columns (into [] (keys data))
           :returning return-column
           })
        (first))))


(defn prepare-insert
  [db spec]
  (let [{:keys [columns sql returning? merge-fn]} (create-insert-sql db spec)]
    (let [prep (.prepare (get-connection db) sql)]
      (reify
        java.lang.AutoCloseable
        (close [_]
          (.close prep))

        clojure.lang.IDeref
        (deref [_]
          prep)

        clojure.lang.IFn
        (invoke [_ data]
          (when-not (map? data)
            (throw (ex-info "only map args supported to prepared insert" {:data data})))

          (let [params (mapv #(get data %) columns)]
            (if returning?
              (merge-fn data (.query prep params))
              (.getRowsAffected (.execute prep params)))
            ))))))


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


(defn create-update-sql
  [{:keys [^TypeRegistry types table-naming column-naming] :as db}
   {:keys [table columns where where-params] :as spec}]
  {:pre [(not (nil? table))
         (vector? columns)
         (seq columns)
         (string? where)
         (vector? where-params)]}
  (let [offset (count where-params)
        table-name (to-sql-name table-naming table)

        column-names (->> columns
                          (map #(to-sql-name column-naming %)))

        where-param-names (->> where-params
                               (map #(to-sql-name column-naming %)))

        columns-fn (fn [input]
                     (mapv #(get input %) columns))

        params-fn (if where-params
                    (fn [input]
                      (mapv #(get input %) where-params))
                    (fn [input]
                      []))

        param-types (->> (concat where-param-names column-names)
                         (map #(.getTypeHandlerForColumn types table-name %))
                         (into []))

        sql (str "UPDATE "
                 (quoted table-name)
                 " SET "
                 (->> column-names
                      (map quoted)
                      (map-indexed (fn [idx col-name]
                                     (str col-name " = $" (+ idx offset 1))))
                      (str/join ", "))
                 (when where
                   (str " WHERE " where)))

        sql (-> (SQL/statement sql)
                (.withName (or (:name spec)
                               (str "update." table-name)))
                (.withTypeRegistry types)
                (.withParamTypes param-types)
                (.create))]
    {:sql sql
     :columns-fn columns-fn
     :params-fn params-fn}))

(defn prepare-update [db spec]
  (let [{:keys [sql columns-fn params-fn]} (create-update-sql db spec)
        prep (.prepare (get-connection db) sql)]

    (reify
      java.lang.AutoCloseable
      (close [_]
        (.close prep))

      clojure.lang.IDeref
      (deref [_]
        prep)

      clojure.lang.IFn
      (invoke [_ data]
        (let [where-params (params-fn data)
              columns (columns-fn data)
              params (into where-params columns)]
          (.getRowsAffected (.execute prep params))))
      (invoke [_ data params]
        (.getRowsAffected (.execute prep (into params (columns-fn data))))))))

(defn update!
  "(sql/update! db :table {:column value, ...} \"id = $1\" id)
   this is also potentially dangerous since all columns are updated"
  ([db table data]
   (update! db table data "1=1" []))
  ([{:keys [table-naming column-naming ^TypeRegistry types] :as db} table data where params]
   {:pre [(sequential? params)]}

    ;; FIXME: use create-update-sql
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
     (let [stmt (as-statement db {:sql sql
                                  ;; type hints for the columns since we know table/column
                                  ;; nil for remainder since we only know expected oid
                                  :params (->> column-names
                                               (map #(.getTypeHandlerForColumn types table-name %))
                                               (concat (repeat (count params) nil)))})]
       (-with-connection db
         (fn [^Connection con]
           ;; must go through prepare because the types of the params are unknown
           ;; reflection not yet supported, the server tells us the types on prepare
           (with-open [prep (.prepare con stmt)]
             (-> prep
                 (.execute (into params (vals data)))
                 (.getRowsAffected)))))))))

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
