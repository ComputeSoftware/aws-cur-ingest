(ns computesoftware.aws-cur.query
  (:require
    [clojure.string :as str]
    [computesoftware.aws-cur.ingest.model :as model.ingest]
    [computesoftware.postgres :as postgres]
    [computesoftware.aws-cur.ingest :as ingest])
  (:import (java.sql Timestamp)))


(defn col-tables [csv-structure]
  {:cols (into {}
           (for [[table {:keys [cols]}] csv-structure
                 col cols
                 :let [[col op & args] (ingest/col-spec col)]]
             [col {:table (name table)
                   op     (case op
                            :fk (name (first args))
                            (or (nil? args) (vec args)))}]))
   :kvs  (into []
           (for [[table {:keys [kvs]}] csv-structure
                 :when kvs]
             [kvs (name table) (str (name table) ":kv")]))})

(def col-info (col-tables model.ingest/csv-structure))

(defn summary-cols [summary-tables-structure]
  (cons (:temporal-aggregate-on summary-tables-structure)
    (map
      (fn [x]
        (cond
          (string? x) x
          (string? (second x)) (second x)
          (vector? x) (recur (first x))))
      (:summaries summary-tables-structure))))

(def costs-col-info
  (reduce
    #(assoc-in % [:cols %2] (assoc (get (:cols col-info) %2) :table "costs"))
    col-info
    (summary-cols ingest/summary-tables-structure)))

(defn refs [csv-structure]
  (reduce
    (fn [m [from to via]]
      (assoc-in m [from to] via))
    {}
    (concat
      (for [[table {:keys [cols kvs]}] csv-structure
            :when kvs
            :let [col (some #(let [[col op arg] (ingest/col-spec %)]
                               (case [op arg] [:fk :bill] col nil)) cols)]
            :when col]
        [(str (name table) ":kv") "bill" col])
      (for [[table {:keys [cols]}] csv-structure
            col cols
            :let [[col op arg] (ingest/col-spec col)]
            :when (= :fk op)]
        [(name table) (name arg) col]))))

(def tables-graph
  (-> (refs model.ingest/csv-structure)
    (assoc "costs"
      (let [{:keys [cols]} col-info]
        (into {}
          (keep #(some-> % cols :fk (vector %)))
          (summary-cols ingest/summary-tables-structure))))))

(defn join-paths [refs]
  (let [single-segment-paths
        (into refs
          (map (fn [[from tos->via]]
                 [from (into tos->via
                         (map (fn [[to via]] [to [via]]))
                         tos->via)]))
          refs)]
    (loop [paths single-segment-paths]
      (if-some [updates (seq (for [[from tos->path] paths
                                   [to path] tos->path
                                   [to' via] (refs to)
                                   :when (nil? (tos->path to'))]
                               [from to' (conj path to via)]))]
        (recur (reduce (fn [paths [from to path]] (assoc-in paths [from to] path))
                 paths updates))
        paths))))

(def all-join-paths (join-paths tables-graph))

(defn- qstr [{:keys [select from where group-by]}]
  (str "SELECT " (str/join ", " select)
    (when (seq from)
      (str "\nFROM " (str/join from)))
    (when (seq where)
      (str "\nWHERE " (str/join "\n AND " where)))
    (when (seq group-by)
      (str "\nGROUP BY " (str/join ", " group-by)))))

;; by construction each columns should appear only once and for the rare occurence
;; where a column is duplicated across tables it's going to hold the same value
;; thus each table (short of intern and kvs) will be present at most once, this
;; simplifies naming/aliasing

;; DON'T FORGET  set enable_partitionwise_join=on;
;; enable_partitionwise_aggregate

(defn get-col-info
  [col-info-lookup col]
  (if-let [{:keys [table] :as col-map} (get (:cols col-info-lookup) col)]
    (assoc col-map
      :type :plain
      :table table
      :col col)
    (some (fn [[re table kv-table]]
            (when (re-matches re col)
              {:type     :kv
               :table    table
               :kv-table kv-table
               :col      col}))
      (:kvs col-info-lookup))))

(defn costs-table-name
  [granularity]
  (case granularity
    (:hourly :daily :monthly) (str "costs:" (name granularity))
    (throw (ex-info (str "Unsupported granulaity " granularity)
             {:granularity granularity}))))

(defn kv-table-alias
  "Returns the alias name for a kv table column"
  [col]
  (str "kv:" col))

(defn table-joins
  [{:keys [root-table plain-col-set kv-cols get-col-input]}]
  (let [core-joins (disj plain-col-set root-table)
        join-paths (all-join-paths root-table)
        join-paths (fn [to] (or (join-paths to) (throw (ex-info (str "No join path from " root-table " to " to) {:from root-table :to to :paths join-paths}))))
        joins-tree (reduce
                     (fn [tree to]
                       (update-in tree (join-paths to) update to merge {})) ; ensure there's a map since a longer branch can have been created first
                     {} core-joins)
        joins-tree (reduce-kv
                     (fn [tree col [table kv-table]]
                       (update-in tree (join-paths table) assoc [kv-table (kv-table-alias col)] {})) ; no need to ensure since there can't be a longer branch through a kv alias
                     joins-tree kv-cols)
        canon-table #(if (string? %) % (first %))
        decl #(if (string? %)
                (ingest/sql-ident %)
                (str (ingest/sql-ident (first %)) " " (ingest/sql-ident (second %))))
        refr (fn [s] (ingest/sql-ident (if (vector? s) (second s) s)))
        kv-ons
        (into {}
          (for [col (keys kv-cols)
                :let [table (ingest/sql-ident (kv-table-alias col))]]
            [table (str " AND " table ".k=" (get-col-input col))]))]
    (for [[table cols] (tree-seq (comp seq val) (comp #(mapcat val %) val) (first {root-table joins-tree}))
          [col tables] cols
          table' (keys tables)
          :when (not= "bill" table')
          :let [kv-on (kv-ons (refr table'))]]
      (str (when kv-on " LEFT") " JOIN " (decl table') " ON " (refr table) "." (ingest/sql-ident col)
        "=" (refr table') ".id"
        (when-some [fkcol (get (tables-graph (canon-table table')) "bill")]
          (str " AND " (refr table') "." (ingest/sql-ident fkcol) "=bill.id"))
        kv-on))))

(defn emit-query [{:keys [from to aggregate by excluding including granularity]}]
  (let [;; next three lines are in case the logic need to be adapted to non summarized tables
        col-info costs-col-info
        root-table "costs"
        cols (set (concat (keys aggregate) by (keys excluding) (keys including)))
        ensure-ident (postgres/ensure-ident-fn)
        all-col-infos (map #(get-col-info col-info %) cols)
        costs-table-name (ingest/sql-ident (costs-table-name granularity))
        plain-cols (into {}
                     (comp (filter (comp #{:plain} :type)) (map (juxt :col :table)))
                     all-col-infos)
        kv-cols (into {}
                  (comp (filter (comp #{:kv} :type)) (map (juxt :col (juxt :table :kv-table))))
                  all-col-infos)
        joins (table-joins {:root-table    root-table
                            :plain-col-set (set (vals plain-cols))
                            :kv-cols       kv-cols
                            :get-col-input #(str "inputs." (ingest/sql-ident "$" %))})
        intern? (fn [col] (or (:intern (get (:cols col-info) col)) (kv-cols col)))
        input-interns (for [s (set (concat (keys kv-cols)
                                     (for [m [including excluding]
                                           [col values] m
                                           :when (intern? col)
                                           v values
                                           :when (not= nil v)]
                                       v)))]
                        (let [alias (ingest/sql-ident (ensure-ident (str "is:" s)))]
                          {:col                             ; the $ is to avoid clash with the other input columns
                           (str "COALESCE(" alias ".id, -1) " (ingest/sql-ident "$" s))
                           :table (str " LEFT JOIN \"interned-string\" " alias " ON "
                                    alias ".text=" (ingest/sql-string s))}))
        canon-cols (-> {}
                     (into (map (fn [[col table]] [col (str (ingest/sql-ident table) "." (ingest/sql-ident col))])) plain-cols)
                     (into (map (fn [[col [table kv-table]]] [col (str (ingest/sql-ident "kv:" col) ".v")])) kv-cols))
        where                                               ; including/excluding
        (for [[op spec] {"" including " NOT" excluding}
              [col values] spec
              :when (seq values)
              :let [values (set values)
                    ccol (canon-cols col)
                    is-null (when (contains? values nil)
                              (str ccol " IS NULL"))
                    in-values (when-some [values (some->> (disj values nil) seq
                                                   (map (if (intern? col)
                                                          #(str "inputs." (ingest/sql-ident "$" %))
                                                          ingest/sql-string))
                                                   (str/join ", "))]
                                (str ccol " IN (" values ")"))]]
          (if (and is-null in-values)
            (str op "(" is-null " OR " in-values ")")
            (str op "(" (or is-null in-values) ")")))
        inputs-query {:select (list*
                                (str (postgres/sql-timestamp from) " \"from\"")
                                (str (postgres/sql-timestamp to) " \"to\"")
                                (map :col input-interns))
                      :from   (cons "(select 1) \"_\"" (map :table input-interns))}]
    {:inputs-query (str (qstr inputs-query) " LIMIT 1")
     :gen-query
     (fn [inputs]
       (let [inputs-table
             (str "(SELECT "
               (str/join ", "
                 (map (fn [[col v]]
                        (str (case col
                               ("from" "to") (postgres/sql-timestamp v)
                               v)
                          " " (ingest/sql-ident col))) inputs)) ") inputs")
             core-query
             {:select   (concat
                          (for [col by]
                            (str (canon-cols col) " " (ingest/sql-ident col)))
                          (for [[col op] aggregate]
                            (str (case op (:min :max :sum) (name op)) "(" (canon-cols col) ") " (ingest/sql-ident col))))
              :from     (list*
                          inputs-table
                          " JOIN bill ON bill.\"bill/BillingPeriodStartDate\" < inputs.to AND bill.\"bill/BillingPeriodEndDate\" > inputs.from"
                          (str " JOIN " costs-table-name
                            " costs ON costs.\"bill/id\"=bill.id AND inputs.from <= costs.\"identity/TimeIntervalStart\" AND costs.\"identity/TimeIntervalStart\" < inputs.to") joins)
              :where    where
              :group-by (range 1 (inc (count by)))}]
         (qstr
           (if-some [intern-by (seq (filter intern? by))]
             (let [other-cols (remove (set intern-by) (concat by (map first aggregate)))
                   interns (for [col intern-by]
                             (let [alias (ingest/sql-ident (ensure-ident (str "is:" col)))]
                               {:col   (str alias ".text " (ingest/sql-ident col))
                                :joins (str " LEFT JOIN \"interned-string\" " alias " ON "
                                         alias ".id=results." (ingest/sql-ident col))}))]
               {:select (concat (map :col interns) (map #(str "results." (ingest/sql-ident %)) other-cols))
                :from   (cons (str "\n (" (qstr core-query) ") results") (map :joins interns))})
             core-query))))}))

(defn query
  "Runs a query (qmap).
   A qmap has the following keys:
   * :granularity amongst :hourly, :daily, and :monthly
   * :from and :to (dates, resp. inclusive exclusive)
   * :by a collection of column names to group by (may include columns stored in kv tables)
   * :aggregate a map of column names to operations (:min :max :sum)
   * :excluding (resp. :including) a map of column names to sets of values,
     to exclude (resp. include only)  matching data for the aggreagtion.
  Returns a collection of maps keyed by column names (as string)."
  [qmap {:keys [tenant-id ^java.sql.Connection conn] :as args}]
  (when-some [missing (seq (remove #(contains? args %) [:tenant-id :conn]))]
    (throw (ex-info (str "Arguments for " (str/join ", " missing) " are missing.") {:args args})))
  (let [schema-id (ingest/schema-ident tenant-id)]
    (with-open [stmt (.createStatement conn)]
      (doto stmt
        (.execute (str "SET search_path TO " schema-id ",public;"))
        (.execute "SET max_parallel_workers_per_gather=16;")
        (.execute "SET enable_partitionwise_join=on;")
        (.execute "SET enable_partitionwise_aggregate=on;"))

      (let [{:keys [inputs-query gen-query]} (emit-query qmap)
            inputs (first (postgres/str-keyed-resultset-seq (.executeQuery stmt inputs-query)))]
        (vec (postgres/str-keyed-resultset-seq (.executeQuery stmt (gen-query inputs))))))))

(comment (sc.api/defsc 55)

  (println (gen-query inputs))

  (def qconn (postgres/get-connection
               {:db-spec {:host     "curdb.cnoppxoie9fj.eu-west-1.rds.amazonaws.com"
                          :dbname   "CUR"
                          :user     "postgres"
                          :password "Baptiste777"}}))
  (def qconn (postgres/get-connection
               {:db-spec {:host   "localhost"
                          :dbname "cur11"}}))
  (time
    (query
      {:granularity :daily
       :from        #inst "2021-08-05T00:00:00Z"
       :to          #inst "2021-08-31T00:00:00Z"
       :by          ["product/ProductName" "identity/TimeIntervalStart"
                     #_"product/servicename"]
       :aggregate   {"lineItem/UnblendedCost" :sum}
       :excluding   {"lineItem/LineItemType" #{"Credit" "Refund"}}}
      {:tenant-id   "cust1"
       :approx-date "2021-08-31"
       :tag         "something"
       :conn        qconn
       "password"   "Baptiste777"}))

  (time
    (query
      {:granularity :daily
       :from        #inst "2021-08-05T00:00:00Z"
       :to          #inst "2021-08-31T00:00:00Z"
       :by          ["product/ProductName" #_"identity/TimeIntervalStart"
                     #_"product/servicename"]
       :aggregate   {"lineItem/UnblendedCost" :sum}
       :excluding   {"lineItem/LineItemType" #{"Credit" "Refund"}}}
      {:tenant-id   "cust1"
       :approx-date "2021-08-31"
       :conn        qconn
       #_#_:tag "something-to-suffix-if-you-perform-several-imports-for-a-given-date"}))

  ;; add an import table listing current (and past?) imports for each tenant
  ;; check no import is running before starting
  ;; if an import is running for too long kill it by disconnecting it and set its state to killed
  ;; set the application name

  )

(comment
  ; bug reported by kenny
  (def qconn (postgres/get-connection
               {:db-spec {:host   "localhost"
                          :dbname "cur-cs"}}))
  (def rs (query
            {:granularity :daily
             :from        #inst "2022-01-01T00:00:00Z"
             :to          #inst "2022-01-03T00:00:00Z"
             :by          ["product/instanceType" "identity/TimeIntervalStart"]
             :aggregate   {"lineItem/UnblendedCost" :sum}
             :excluding   {"lineItem/LineItemType" #{"Credit"}}}
            (assoc base-argm :conn qconn)))

  (filter (fn [m]
            (and
              (= (inst-ms (m "identity/TimeIntervalStart")) (inst-ms #inst"2022-01-01"))
              (= (m "product/instanceType") "t3.xlarge"))) rs)

  (def rs2 (query
             {:granularity :daily
              :from        #inst "2022-01-01T00:00:00Z"
              :to          #inst "2022-01-03T00:00:00Z"
              :by          ["lineItem/LineItemType" "identity/TimeIntervalStart"]
              :aggregate   {"lineItem/UnblendedCost" :sum}
              :including   {"product/instanceType" ["t3.xlarge"]}}
             (assoc base-argm :conn qconn)))

  (def rs3 (query
             {:granularity :daily
              :from        #inst "2022-01-01T00:00:00Z"
              :to          #inst "2022-01-03T00:00:00Z"
              :by          ["product/instanceType" "identity/TimeIntervalStart" "lineItem/LineItemType"]
              :aggregate   {"lineItem/UnblendedCost" :sum}
              #_#_:including {"lineItem/LineItemType" #{"Usage"}}}
             (assoc base-argm :conn qconn)))

  (filter (fn [m]
            (and
              (= (inst-ms (m "identity/TimeIntervalStart")) (inst-ms #inst"2022-01-01"))
              (= (m "product/instanceType") "t3.xlarge"))) rs3)

  (count rs)
  )

(defn filter-options-sql-params
  "Returns the sql-params to get the filter options for the given
  ListFilterOptionsRequest."
  [request]
  (let [{:keys [granularity filter-category start stop]} request
        costs-table (costs-table-name (-> granularity name keyword))
        filter-col-info (get-col-info costs-col-info filter-category)
        kv? (= :kv (:type filter-col-info))
        filter-col-name (if kv?
                          (str (-> filter-col-info :col kv-table-alias postgres/sql-ident) ".v")
                          (str (postgres/sql-ident (:table filter-col-info)) "." (postgres/sql-ident (:col filter-col-info))))
        start-t (Timestamp. (inst-ms start))
        end-t (Timestamp. (inst-ms stop))

        joins (table-joins
                {:root-table    "costs"
                 :plain-col-set (cond-> #{"costs"}
                                  (not kv?) (conj (:table filter-col-info)))
                 :kv-cols       (cond-> {}
                                  kv?
                                  (assoc
                                    (:col filter-col-info)
                                    [(:table filter-col-info) (:kv-table filter-col-info)]))
                 :get-col-input (fn [col]
                                  (str
                                    "("
                                    "SELECT id FROM \"interned-string\""
                                    " WHERE text=" (postgres/sql-constant col)
                                    ")"))})]
    [(str
       "SELECT DISTINCT \"interned-string\".text AS \"category-value/name\" "
       " FROM " (postgres/sql-ident costs-table) " AS costs"

       " JOIN bill AS bill ON"
       " bill.\"bill/BillingPeriodStartDate\" < ?"
       " AND bill.\"bill/BillingPeriodEndDate\" > ?"

       ;; Resolve table refs
       (str/join "" joins)

       ;; Resolve interned string ids
       " JOIN \"interned-string\" ON"
       " \"interned-string\".id = " filter-col-name

       " WHERE"
       " costs.\"bill/id\" = \"bill\".id"
       " AND ? <= costs.\"identity/TimeIntervalStart\""
       " AND costs.\"identity/TimeIntervalStart\" < ?"
       " LIMIT 1000")
     end-t
     start-t
     start-t
     end-t]))
