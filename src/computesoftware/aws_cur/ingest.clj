(ns computesoftware.aws-cur.ingest
  (:require
    [clojure.instant :as instant]
    [clojure.java.io :as io]
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [com.climate.claypoole :as cp]
    [computesoftware.aws-cur.ingest.model :as model.ingest]
    [computesoftware.csv-scanner :as csv]
    [computesoftware.postgres :as postgres]
    [computesoftware.spec-helpers :as spech])
  (:import (java.io File)))

(set! *warn-on-reflection* true)

(spech/sdef ::tenant-id
  "Tenant/customer identifier used to create tenant schema."
  ::spech/not-blank-string)

(spech/sdef ::approx-date
  "The approximate max data contained in the CUR data being imported. Used to
  allocate partitions. "
  (s/and ::spech/not-blank-string #(re-matches #"\d{4}-\d{2}-\d{2}" %)))

(spech/sdef ::input-stream-fns
  "Collection of thunks returning an InputStream of GZIP CUR CSV data."
  (s/coll-of fn?))

(spech/sdef ::get-conn
  "Thunk returning a java.sql.Connection. Connections returned are managed by
  this ingestion process."
  fn?)

(spech/sdef ::cur-id
  "Unique identifier for a CUR within a single tenant. The ID must be globally
  unique for the given tenant. A tenant can have multiple cur-ids."
  ::spech/not-blank-string)

(spech/sdef ::import-id
  "The unique identifier for an import of a CUR. A given CUR-month dataset is
  unique by tenant-id + cur-id + import-id. Typically this value is created by
  the yyyy-mm-dd date the import runs on."
  ::spech/not-blank-string)

;; ingestion pseudocode:
;; for each line of the CSV, do:
;;   if LineItemId is new then (it will be new < 10%)
;;     if Bill is new then ... (it will be new < epsilon)
;;     if SKU is new then insert product
;;     if Pricing is new then ...
;;     insert LineItem
;;   endif
;;   insert LineItemDetail

;; how to check if something is new: look in the atom
;; Note: sku & pricing may be initialiazed from DB (or lazy on upsert?)
;; Note 2: sku has two states as sometimes we have no details for a product

;; BILLS
;; "bill/BillType"
;;  "bill/BillingEntity"
;;  "bill/BillingPeriodEndDate"
;;  "bill/BillingPeriodStartDate"
;;  "bill/InvoiceId"
;; "bill/PayerAccountId"

;; ### Transient Data during Import

;; `Bill` has no `InvoiceId` until the report is final. So **for `Bill` there won't be some unicity constraints**.

;; The idea is that during an import, new bills are created as needed but with an extra flag field `status` (values amongst `importing`, `current` and `obsolete`).

;; **When querying only `current` bills are considered.**

;; Upon completion of the import, a transaction atomically performs these two changes:

;; - switches bills (in case of multi-tenancy for the account whose CUR is being imported only) whose `status` is `current` **and `InvoiceId` blank** to `obsolete`,
;; - switches freshly imported bills from `importing` to `current`.

;; Afterwards and outside this transaction `obsolete` `Bills` along with associated `LineItems` and `LineItemDetails` can be deleted.

(declare ^:dynamic *partitions-count*)
(declare ^:dynamic *approx-date*)
(def ^:dynamic *import-suffix*)

(defn bytes-eq [^bytes a1 from1 to1 ^bytes a2 from2 to2]
  (let [from1 (long from1)
        to1 (long to1)
        from2 (long from2)
        to2 (long to2)]
    (and (= (- to1 from1) (- to2 from2))
      (= (java.nio.ByteBuffer/wrap a1 from1 (- to1 from1))
        (java.nio.ByteBuffer/wrap a2 from2 (- to2 from2))))))

(def debug false)

(defmacro dbg-prn
  "Evaluates expr and prints the time it took.  Returns the value of
 expr."
  {:added "1.0"}
  [& more]
  (when debug
    (let [l (str "[" (:line (meta &form)) "]")]
      `(prn ~l ~@more))))

(defmacro dbg-time
  "Evaluates expr and prints the time it took.  Returns the value of
 expr."
  {:added "1.0"}
  [expr]
  (if debug
    `(let [start# (. System (nanoTime))
           ret# ~expr]
       (prn (str "Elapsed time: " (/ (double (- (. System (nanoTime)) start#)) 1000000.0) " msecs"))
       ret#)
    expr))

(defn parse-resource-tag
  "Given a CUR resource tag formatted string, returns a map of :tag-type and
  :tag-key if the string is a resourceTag string, else nil."
  [s]
  (when-let [[_ tag-k-s] (re-find model.ingest/resource-tag-re s)]
    (let [[tag-type tag-key] (str/split tag-k-s #":" 2)]
      {:tag-type tag-type
       :tag-key  tag-key})))

(comment
  (parse-resource-tag "resourceTags/user:datomic:system"))

(def summary-tables-structure
  "This structure controls how summary (aggregate) tables are created.
   It's even more adhoc to this project than csv-structure.
   :on, [[table1 col1] [table2 col2]] which specify the core join of the aggregation
   :temporal-aggregate-on (string) the sql column to use for temporal grouping
   :summaries, a collection of agg-specs.
   Agg-specs are either:
   * a sql column name (string), will be part of the grouping key
   * a [:table-name column-name] which acts like a string alone but halps disambiguate
     between homonymous columns across tables, part of the grouping key
   * a [col op] where op is a symbol amongst 'max, 'min, 'sum"
  {:on                    [[:line-item "id"] [:line-item-details "lineItem/id"]]
   :temporal-aggregate-on "identity/TimeIntervalStart"
   :summaries             [["identity/TimeIntervalEnd" 'max]
                           [:line-item "bill/id"]
                           "line-item-attributes/id"
                           ["lineItem/UsageStartDate" 'min]
                           ["lineItem/UsageEndDate" 'max]
                           ["pricing/publicOnDemandCost" 'sum]
                           ["lineItem/BlendedCost" 'sum]
                           ["lineItem/UnblendedCost" 'sum]
                           ["lineItem/UsageAmount" 'sum]
                           ["lineItem/NormalizedUsageAmount" 'sum]
                           ["savingsPlan/SavingsPlanEffectiveCost" 'sum]
                           ["savingsPlan/UsedCommitment" 'sum]
                           ["reservation/AmortizedUpfrontCostForUsage" 'sum]
                           ["reservation/EffectiveCost" 'sum]
                           ["reservation/RecurringFeeForUsage" 'sum]]})

(def ^String sql-ident postgres/sql-ident)
(def ^String sql-string postgres/sql-constant)

(defn col-spec [col]
  (if (sequential? col)
    col
    [col :intern]))

(defn partitions-ranges [partitions-count ^String approx-date granularity]
  (let [t (-> approx-date ^java.util.Date clojure.instant/read-instant-date .toInstant (java.time.LocalDateTime/ofInstant java.time.ZoneOffset/UTC))
        from (.withDayOfMonth t 1)
        to (.plusDays t 1)
        d (java.time.Duration/between from to)
        N (long partitions-count)]
    (if (= :monthly granularity)
      [[nil nil]]
      (partition 2 1
        (concat
          [nil]
          (distinct
            (->>
              (for [i (range (inc N))]
                (-> from (.plus (-> d (.multipliedBy i) (.dividedBy N)))
                  (.truncatedTo (case granularity
                                  :daily java.time.temporal.ChronoUnit/DAYS
                                  :hourly java.time.temporal.ChronoUnit/HOURS
                                  :max java.time.temporal.ChronoUnit/NANOS))
                  postgres/sql-timestamp))
              distinct
              (drop 1)
              drop-last))
          [nil])))))

(defn partitions-list [[table-name {:keys [cols key kvs sub-partition-by global-id]}] suffix]
  (let [partitions-count *partitions-count*
        approx-date *approx-date*
        suffixed-table-name (cond-> table-name sub-partition-by (str suffix))]
    #_(cons {:pre (str "CREATE TABLE IF NOT EXISTS " (sql-ident table-name) " (LIKE " (sql-ident suffixed-table-name)
                    ") PARTITION BY LIST(\"bill/id\");")})
    (case (first sub-partition-by)
      :hash
      (for [i (range partitions-count)]
        [suffixed-table-name (str suffixed-table-name ":" i)
         :hash {:mod partitions-count :rem i}])
      :range
      (map-indexed
        (fn [i [from to]]
          [suffixed-table-name (str suffixed-table-name ":" i)
           :range {:from (or from "MINVALUE") :to (or to "MAXVALUE")}])
        (partitions-ranges partitions-count approx-date :max)))))

(defn index-ddls
  [{:keys [indexes tables]}]
  (for [[index-tag {:keys [method cols include]}] indexes
        table-name tables]
    {:post
     (str "CREATE INDEX IF NOT EXISTS "
       (sql-ident table-name ":idx:" index-tag)
       " ON " (sql-ident table-name)
       " USING " (case method (:btree :hash) (name method)) "("
       (str/join ", " (map (comp sql-ident name) cols)) ")"
       (when include
         (str " INCLUDE (" (str/join ", " (map (comp sql-ident name) include)) ")")))}))

(defn ddls [[table-name {:keys [cols key kvs sub-partition-by global-id ref-data custom-ddls indexes] :as table-spec}] suffix]
  (let [table-name (name table-name)
        partitioned-by-bill (not (or #_global-id ref-data))
        partition-col (when partitioned-by-bill
                        (case table-name
                          "bill" "id"
                          "\"bill/id\""))
        suffixed-table-name (cond-> table-name partitioned-by-bill (str suffix))]
    (cond->
      (cons
        {:tables [suffixed-table-name]
         :pre    (with-out-str
                   (println (cond-> "CREATE TABLE" (not partitioned-by-bill) (str " IF NOT EXISTS")) (sql-ident suffixed-table-name) "(")
                   (when (seq key)
                     (print "  id integer"))                ; don't enforce primary key
                   (reduce
                     (fn [comma col]
                       (when comma (println ","))
                       (let [[col-name op arg] (col-spec col)]
                         (print " " (sql-ident col-name)
                           (case op
                             :fk "integer" #_(print-str "integer REFERENCES" (sql-ident (name arg)))
                             :intern "integer" #_"integer REFERENCES \"interned-string\""
                             :text "text"
                             :numeric "double precision"
                             :datetime "timestamp"
                             #_#_:datetime-range "tsrange")))
                       true)
                     (seq key) cols)
                   (newline)
                   (when-some [[type & cols] (seq sub-partition-by)]
                     (print (str ") PARTITION BY " (case type :hash "HASH" :range "RANGE") " (" (str/join ", " (map sql-ident cols)))))
                   (println ");"))}
        (concat
          custom-ddls
          ;; DDL for :indexes key
          (index-ddls {:indexes indexes :tables (set [table-name suffixed-table-name])})

          ;; DDL for fk cols
          (for [col cols
                :let [[col-name op] (col-spec col)]
                :when (case op
                        :fk (and sub-partition-by (not= col-name "bill/id")) ; no fk index for bill/id when table is already partitioned by bill/id
                        #_#_:datetime-range true
                        false)]
            {:post
             (case op
               :fk
               (str "CREATE INDEX IF NOT EXISTS " (sql-ident suffixed-table-name ":" col-name) " ON "
                 (sql-ident suffixed-table-name) " USING hash (" (sql-ident col-name) ");\n")
               #_#_:datetime-range                          ; bad perf
                       (str "CREATE INDEX IF NOT EXISTS " (sql-ident suffixed-table-name ":" col-name) " ON "
                         (sql-ident suffixed-table-name) " USING gist (" (sql-ident col-name) ");\n"))})))

      global-id
      (concat [{:pre (str "CREATE SEQUENCE IF NOT EXISTS " (sql-ident table-name ":seq") ";\n")}])

      (seq key)
      (concat                                               ; replace the primary key index
        (for [table (cond-> [suffixed-table-name] partitioned-by-bill (conj table-name))
              :let [using-cols (if (and partitioned-by-bill (not= "bill" table-name))
                                 " (\"bill/id\", id);\n"
                                 " USING hash (id);\n")]]
          {:post (str "CREATE INDEX IF NOT EXISTS " (sql-ident table ":id") " ON "
                   (sql-ident table) using-cols)}))

      partitioned-by-bill
      (concat (cons {:tables [table-name]
                     :pre    (str "CREATE TABLE IF NOT EXISTS " (sql-ident table-name) " (LIKE " (sql-ident suffixed-table-name) ") PARTITION BY LIST(" partition-col ");")}
                (when sub-partition-by
                  (let [partitions-count *partitions-count*
                        approx-date (str (sql-string *approx-date*) "::timestamp")]
                    (for [[partitioned-table partition-table type opts] (partitions-list [table-name table-spec] suffix)]
                      (case type
                        :hash
                        {:tables [partition-table]
                         :pre    (str "CREATE TABLE IF NOT EXISTS " (sql-ident partition-table) " PARTITION OF "
                                   (sql-ident partitioned-table) " FOR VALUES WITH (MODULUS " (:mod opts) ", REMAINDER " (:rem opts) ");\n")}
                        :range
                        {:tables [partition-table]
                         :pre    (str "CREATE TABLE IF NOT EXISTS " (sql-ident partition-table) " PARTITION OF "
                                   (sql-ident partitioned-table) " FOR VALUES FROM (" (:from opts)
                                   ") TO (" (:to opts) ");\n")}))))))

      kvs
      (concat
        (let [suffixed-kv-table (str suffixed-table-name ":kv")
              kv-table (str table-name ":kv")]
          (concat
            [{:tables [suffixed-kv-table]
              :pre    (str "CREATE TABLE IF NOT EXISTS " (sql-ident suffixed-kv-table)
                        "(" (some-> partition-col (str " integer not null, ")) "id integer not null, k integer not null, v integer not null);\n")}
             (when partitioned-by-bill
               {:tables [kv-table]
                :pre    (str "CREATE TABLE IF NOT EXISTS " (sql-ident kv-table) " (LIKE " (sql-ident suffixed-kv-table) ") PARTITION BY LIST(" partition-col ");")})]
            (for [table-name (cond-> [suffixed-table-name] partitioned-by-bill (conj table-name))
                  create-index
                  [(str "CREATE INDEX IF NOT EXISTS " (sql-ident table-name ":kv:ik") " ON " (sql-ident table-name ":kv") "(" (some-> partition-col (str ", ")) "id, k) INCLUDE (v);\n")
                   (str "CREATE INDEX IF NOT EXISTS " (sql-ident table-name ":kv:kv") " ON " (sql-ident table-name ":kv") "(" (some-> partition-col (str ", ")) "k, v) INCLUDE (id);\n")]]
              {:post create-index})))))))

(defn- index-edges [edges]
  (reduce (fn [m [from to]] (assoc m from (conj (m from #{}) to))) {} edges))

(defn topo-sort [edges-index]
  (loop [index edges-index result []]
    (let [dead-ends (index-edges (for [[from tos] index
                                       to tos
                                       :when (empty? (index to))]
                                   [to from]))]
      (cond
        (seq dead-ends)
        (recur (reduce
                 (fn [index [to froms]]
                   (reduce #(update %1 %2 disj to) (dissoc index to) froms))
                 index dead-ends)
          (into result (keys dead-ends)))
        (some seq (vals index)) (throw (Exception. "Cycle!"))
        :else (into result (keys index))))))

(def compile-col-spec nil)
(defmulti compile-col-spec (fn [env [_ op] _] op))

(defmethod compile-col-spec :text [env [sql-name _ csv-name] {:keys [write-text]}]
  (let [^computesoftware.csv_scanner.Scanner scanner (:scanner env)]
    (if-some [csv-idx (get (.headers scanner) (or csv-name sql-name))]
      (let [csv-index (int csv-idx)]
        #(let [^String s (.nth scanner csv-index)]
           (write-text % (when-not (.isEmpty s) s))))
      (do
        (print "Column" (or csv-name sql-name) "is missing from the CSV.")
        #(write-text % nil)))))

(defmethod compile-col-spec :intern [env [sql-name _ csv-name] {:keys [write-int]}]
  (let [^computesoftware.csv_scanner.Scanner scanner (:scanner env)
        col-name (or csv-name sql-name)]
    (if (get (.headers scanner) col-name)
      (let [sk (csv/scan-key scanner [col-name])
            intern (-> env :intern-table :intern-fn)]
        #(write-int % (intern sk)))
      (do
        (print "Column" col-name "is missing from the CSV.")
        #(write-int % nil)))))

(defmethod compile-col-spec :numeric [env [sql-name _ csv-name] {:keys [write-double]}]
  (let [^computesoftware.csv_scanner.Scanner scanner (:scanner env)]
    (if-some [csv-idx (get (.headers scanner) (or csv-name sql-name))]
      (let [csv-index (int csv-idx)]
        #(let [^String s (.nth scanner csv-index)]
           (write-double % (when-not (.isEmpty s) (Double/parseDouble s)))))
      (do
        (print "Column" (or csv-name sql-name) "is missing from the CSV.")
        #(write-double % nil)))))

(defmethod compile-col-spec :fk [env [sql-name _ table-name] {:keys [write-int]}]
  (let [{:keys [row-fn]} (-> env :tables (get table-name))]
    #(write-int % (row-fn))))

#_(defmethod compile-col-spec :datetime-range [env [sql-name _ csv-from csv-to] {:keys [write-range]}]
    (let [^computesoftware.csv_scanner.Scanner scanner (:scanner env)
          csv-from (int (get (.headers scanner) (or csv-from sql-name)))]
      (if csv-to
        (let [csv-to (int (get (.headers scanner) csv-to))]
          #(let [^String from (.nth scanner csv-from)
                 ^String to (.nth scanner csv-to)]
             (write-range % (when-not (.isEmpty from) from) (when-not (.isEmpty to) to))))
        #(let [^String from (.nth scanner csv-from)]
           (when-not (.isEmpty from)
             (let [i (.indexOf from (int \/))]
               (write-range % (subs from 0 i) (subs from (inc i)))))))))

(defmethod compile-col-spec :datetime [env [sql-name _ csv-name start-or-end-or-nil] {:keys [write-timestamp]}]
  (let [^computesoftware.csv_scanner.Scanner scanner (:scanner env)
        csv-idx (int (get (.headers scanner) (or csv-name sql-name)))
        extract (case start-or-end-or-nil
                  :start (fn [^String s] (.substring s 0 (.indexOf s (int \/))))
                  :end (fn [^String s] (.substring s (inc (.indexOf s (int \/)))))
                  nil identity)]
    #(let [^String t (.nth scanner csv-idx)]
       (when-not (.isEmpty t) (write-timestamp % (extract t))))))

(defn compile-kv-spec [{{intern :intern-fn} :intern-table :as env} csv-name {:keys [write-int init-tuple terminate-tuple]} partitioned-by-bill]
  (let [^computesoftware.csv_scanner.Scanner scanner (:scanner env)
        colid (intern (csv/string-key (.charset scanner) csv-name))
        sk (csv/scan-key scanner [csv-name])
        bill-id (some-> env :tables :bill :row-fn)]
    (if partitioned-by-bill
      (fn [table-acc id]
        (if-some [value-id (intern sk)]
          (let [tuple-acc
                (-> table-acc
                  init-tuple
                  (write-int (bill-id))
                  (write-int id)
                  (write-int colid)
                  (write-int value-id))]
            (terminate-tuple table-acc tuple-acc))
          table-acc))
      (fn [table-acc id]
        (if-some [value-id (intern sk)]
          (let [tuple-acc
                (-> table-acc
                  init-tuple
                  (write-int id)
                  (write-int colid)
                  (write-int value-id))]
            (terminate-tuple table-acc tuple-acc))
          table-acc)))))

(defn compile-kvs-spec [env kpattern table-writers kv-copy partitioned-by-bill]
  (let [{:keys [init-table terminate-table] :as table-writer}
        (table-writers (if partitioned-by-bill ["bill/id" "id" "k" "v"] ["id" "k" "v"]) kv-copy)
        state (volatile! (init-table))
        ^computesoftware.csv_scanner.Scanner scanner (:scanner env)
        kv-fns (vec (for [[colname id] (.headers scanner)
                          :when (re-matches kpattern colname)]
                      (compile-kv-spec env colname table-writer partitioned-by-bill)))]
    {:kvs-fn
     (fn [id]
       (vreset! state (reduce (fn [acc f] (f acc id)) @state kv-fns)))
     :terminate #(terminate-table @state)}))

(defn compile-when-some-pred [env kpattern]
  (let [^computesoftware.csv_scanner.Scanner scanner (:scanner env)
        sk (csv/scan-key scanner (for [colname (keys (.headers scanner))
                                       :when (re-matches kpattern colname)]
                                   colname))]
    #(not (csv/empty-key? sk))))

(defn mk-ensure-local-id
  "Takes a scan key and a side-effecting callback of two arguments: id and persistent key.
   Returns the id for the current value of the scan key or nil if the key is empty."
  ([ks-to-ids on-allocate]
   (fn [sk]
     (when-not (csv/empty-key? sk)
       (let [v @ks-to-ids]
         (or (v sk)
           (let [k @sk]
             (loop [v v]
               (let [id (count v)]
                 (if (compare-and-set! ks-to-ids v (assoc v k id))
                   (doto id (on-allocate k))
                   (let [v @ks-to-ids]
                     (or (v k) (recur v))))))))))))
  ([ks-to-ids sk on-allocate]
   (partial (mk-ensure-local-id ks-to-ids on-allocate) sk)))

(defn mk-ensure-stateful-id
  ([whensome? ks-to-ids on-allocate]
   (fn [sk]
     (when-not (csv/empty-key? sk)
       (let [v @ks-to-ids
             [k id+state] (find v sk)]
         ; id+state is id*2 + boolean state (1: not allocated, 0: allocated)
         (cond
           ; if not yet allocated and available data
           (and (not (some-> id+state even?)) (whensome?))
           (let [k (or k @sk)]
             (loop [v v]
               (let [id (or (some-> id+state (quot 2)) (count v))]
                 (if (compare-and-set! ks-to-ids v (assoc v k (* 2 id)))
                   (doto id (on-allocate k))
                   (let [v @ks-to-ids
                         id+state (v k)]
                     (if (some-> id+state even?)
                       (quot id+state 2)
                       (recur v)))))))
           ; nothing new
           id+state (quot id+state 2)
           :else                                            ; new unallocated id
           (loop [v v]
             (let [k @sk
                   id (count v)]
               (if (compare-and-set! ks-to-ids v (assoc v k (inc (* 2 id))))
                 id
                 (let [v @ks-to-ids]
                   (or (some-> (v k) (quot 2))
                     (recur v)))))))))))
  ([whensome? ks-to-ids sk on-allocate]
   (partial (mk-ensure-stateful-id whensome? ks-to-ids on-allocate) sk)))

(defn mk-ensure-global-id
  "Takes a scan key and a side-effecting callback of two arguments: id and persistent key.
   Returns the id for the current value of the scan key or nil if the key is empty."
  ([nextval ks-to-ids on-allocate]
   (fn [sk]
     (when-not (csv/empty-key? sk)
       (let [v @ks-to-ids]
         (or (v sk)
           (let [id (nextval)                               ; nextval must generate unique ids across threads
                 k @sk]
             (swap! ks-to-ids assoc k id)
             (doto id (on-allocate k))))))))
  ([nextval ks-to-ids sk on-allocate]
   (partial (mk-ensure-global-id nextval ks-to-ids on-allocate) sk)))

(defn compile-intern [{:keys                           [interns]
                       {{copy :main} :interned-string} :copies} table-writers]
  (let [{:keys [init-table terminate-table init-tuple terminate-tuple write-int write-text] :as table-writer}
        (table-writers ["id" "text"] copy)
        state (volatile! (init-table))]
    {:intern-fn
     (mk-ensure-local-id interns
       (fn [id ^computesoftware.csv_scanner.ICsvKey k]
         (vreset! state (-> @state init-tuple (write-int id) (write-text (nth (.row k) (aget (.runs k) 0)))
                          (terminate-tuple @state)))))
     :terminate #(terminate-table @state)}))

(defn summary-tables-ddls [table-prefix table-spec]
  (let [import-suffix *import-suffix*
        subtable-prefix (str table-prefix import-suffix)
        tcol (sql-ident (:temporal-aggregate-on table-spec))
        as-col-name #(cond
                       (string? %) (sql-ident %)
                       (and (vector? %) (string? (second %)))
                       (str (sql-ident (name (first %)) import-suffix) "." (sql-ident (second %))))
        as-simple-col-name #(cond
                              (string? %) (sql-ident %)
                              (and (vector? %) (string? (second %))) (sql-ident (second %)))
        as-result-col-name #(or (as-simple-col-name %) (recur (first %)))
        result-col-names (into [tcol]
                           (map #(or (as-result-col-name %) (as-result-col-name (first %))))
                           (:summaries table-spec))
        hourly-exprs (into []
                       (map (fn [col-spec]
                              (or (as-col-name col-spec)
                                (when (coll? col-spec)
                                  (let [[col f] col-spec]
                                    (str f "(" (as-col-name col) ")"))))))
                       (:summaries table-spec))
        daily+-exprs (into []
                       (map (fn [col-spec]
                              (or (as-simple-col-name col-spec)
                                (when (coll? col-spec)
                                  (let [[col f] col-spec]
                                    (str f "(" (as-simple-col-name col) ")"))))))
                       (:summaries table-spec))
        group-by-cols (into [1]
                        (keep-indexed (fn [i col-spec]
                                        (when (as-col-name col-spec) (+ 2 i))))
                        (:summaries table-spec))

        hourly-partition-ranges (partitions-ranges *partitions-count* *approx-date* :hourly)
        daily-partition-ranges (partitions-ranges *partitions-count* *approx-date* :daily)

        ;; Table names
        hourly-subtable-nf #(str subtable-prefix ":hourly:" %)
        hourly-table-n (str table-prefix ":hourly")
        daily-subtable-nf #(str subtable-prefix ":daily:" %)
        daily-table-n (str table-prefix ":daily")
        monthly-table-n (str table-prefix ":monthly")]
    [; hourly sub-table
     {:tables (map hourly-subtable-nf (range (count hourly-partition-ranges)))
      :post
      (map-indexed
        (fn [i [from to]]
          (let [table (sql-ident (hourly-subtable-nf i))]
            (list
              ; create hourly subpartition
              (str "CREATE TABLE " table " (" (str/join ", " result-col-names) ") AS SELECT "
                (str/join ", " (cons (str "date_trunc('hours', " tcol ")") hourly-exprs))
                " FROM " (str/join ", " (map #(sql-ident (name (first %)) import-suffix) (:on table-spec)))
                " WHERE " (let [[c1 c2] (:on table-spec)]
                            (str (as-col-name c1) "=" (as-col-name c2)))
                (when from (str " AND " from " <= " tcol))
                (when to (str " AND " tcol " < " to))
                " GROUP BY " (str/join ", " group-by-cols))
              ; btree index on time
              (str "CREATE INDEX ON " table "(" tcol ")"))))
        hourly-partition-ranges)}
     ;; hourly
     {:tables [hourly-table-n
               (str subtable-prefix ":hourly")]
      :post
      (concat
        [; create partitioned hourly root table
         (str "CREATE TABLE IF NOT EXISTS " (sql-ident hourly-table-n) "(LIKE "
           (sql-ident subtable-prefix ":hourly:0") ") PARTITION BY LIST (\"bill/id\")")
         ; create partitioned hourly subtable for the import
         (str "CREATE TABLE " (sql-ident subtable-prefix ":hourly") "(LIKE "
           (sql-ident hourly-table-n) ") PARTITION BY RANGE (" tcol ");")]
        ; attach all hourly partitions to the subtable
        (map-indexed
          (fn [i [from to]]
            (str "ALTER TABLE " (sql-ident subtable-prefix ":hourly")
              " ATTACH PARTITION " (sql-ident subtable-prefix ":hourly:" i)
              " FOR VALUES FROM (" (or from "MINVALUE") ") TO (" (or to "MAXVALUE") ");"))
          hourly-partition-ranges))}
     ; daily sub-table
     {:tables (map daily-subtable-nf (range (count daily-partition-ranges)))
      :post
      (map-indexed
        (fn [i [from to]]
          (str "CREATE TABLE " (sql-ident (daily-subtable-nf i)) " (" (str/join ", " result-col-names) ") AS SELECT " (str/join ", " (cons (str "date_trunc('days', " tcol ")") daily+-exprs))
            " FROM " (sql-ident subtable-prefix ":hourly")
            " WHERE true "
            (when from (str " AND " from " <= " tcol))
            (when to (str " AND " tcol " < " to))
            " GROUP BY " (str/join ", " group-by-cols)))
        daily-partition-ranges)}
     {:tables [daily-table-n
               (str subtable-prefix ":daily")]
      :post
      (concat
        [; create partitioned daily root table
         (str "CREATE TABLE IF NOT EXISTS " (sql-ident daily-table-n) " (LIKE "
           (sql-ident hourly-table-n) ") PARTITION BY LIST (\"bill/id\")")
         ; create partitioned daily subtable for the import
         (str "CREATE TABLE " (sql-ident subtable-prefix ":daily") "(LIKE "
           (sql-ident daily-table-n) ") PARTITION BY RANGE (" tcol ")")]
        ; attach all daily partitions to the subtable
        (map-indexed
          (fn [i [from to]]
            (str "ALTER TABLE " (sql-ident subtable-prefix ":daily")
              "ATTACH PARTITION " (sql-ident (daily-subtable-nf i))
              " FOR VALUES FROM (" (or from "MINVALUE") ") TO (" (or to "MAXVALUE") ");"))
          daily-partition-ranges))}
     ; monthly
     {:tables [monthly-table-n
               (str subtable-prefix ":monthly")]
      :post
      [; create partitioned daily root table
       (str "CREATE TABLE IF NOT EXISTS " (sql-ident monthly-table-n) "(LIKE "
         (sql-ident daily-table-n) ") PARTITION BY LIST (\"bill/id\")")
       ; create monthly (unattached) partition for the import
       (str "CREATE TABLE " (sql-ident subtable-prefix ":monthly") " (" (str/join ", " result-col-names) ") AS SELECT " (str/join ", " (cons (str "date_trunc('months', " tcol ")") daily+-exprs))
         " FROM " (sql-ident subtable-prefix ":daily")
         " GROUP BY " (str/join ", " group-by-cols))]}]))

(defn compile-table-spec [{:keys [scanner ids copies sql-exec] :as env} table-name {:keys [cols key kvs global-id sub-partition-by ref-data] whensome :when-some} table-writers]
  (let [{main-copy :main kv-copy :kvs} (copies table-name)
        cols (map col-spec cols)
        sqlcolnames (cond->> (map first cols) (seq (filter string? key)) (cons "id"))
        {:keys [kvs-fn] terminate-kvs :terminate kvs-state :state} (when kvs (compile-kvs-spec env kvs table-writers kv-copy (not ref-data)))
        whensome? (some->> whensome (compile-when-some-pred env))
        {:keys [init-table terminate-table init-tuple terminate-tuple write-int] :as table-writer} (table-writers sqlcolnames main-copy)
        mk-ensure-id
        (cond
          global-id
          (let [nextval (sql-exec (fn [^java.sql.Connection conn]
                                    (let [ps (.prepareStatement conn (str "SELECT nextval('" (name table-name) ":seq');"))]
                                      #(sql-exec
                                         (fn [_]
                                           (-> ps .executeQuery (doto .next) (.getInt 1)))))))]
            (partial mk-ensure-global-id nextval))
          whensome? (partial mk-ensure-stateful-id whensome?)
          :else
          mk-ensure-local-id)
        scanner (:scanner env)
        field-fns (into [] (map #(compile-col-spec env % table-writer)) cols)
        state (volatile! (init-table))
        row-fn (if (seq key)
                 (let [sk (csv/scan-key scanner key)
                       ids-atom (-> env :ids (get table-name))
                       mk-tuple
                       (fn [id _]
                         (vswap! state terminate-tuple
                           (reduce (fn [tuple-state f] (f tuple-state))
                             (write-int (init-tuple @state) id) field-fns)))
                       mk-tuple (if kvs-fn
                                  (fn [id _]
                                    (mk-tuple id _)
                                    (kvs-fn id))
                                  mk-tuple)]
                   (mk-ensure-id ids-atom sk mk-tuple))
                 #(vswap! state terminate-tuple
                    (reduce (fn [tuple-state f] (f tuple-state)) (init-tuple @state) field-fns)))]
    {:sql-cols  sqlcolnames
     :row-fn    row-fn
     :terminate #(cond-> {:main (terminate-table @state)}
                   terminate-kvs (assoc :kvs (terminate-kvs)))}))

(def debug-table-writer
  "Default writers mosty meant for repl/demo/debug purposes."
  (constantly
    {:init-table      (fn [] (transient [#_(vec colnames)]))
     :init-tuple      (fn [_] (transient []))
     :terminate-tuple (fn [table tuple]
                        (conj! table (persistent! tuple)))
     :terminate-table persistent!
     :write-text      conj!
     :write-double    conj!
     :write-int       conj!
     :write-timestamp conj!
     :write-range     (fn [acc from to] (conj! acc [from to]))}))

;; define RANGE_EMPTY			0x01	/* range is empty */
;; #define RANGE_LB_INC		0x02	/* lower bound is inclusive */
;; #define RANGE_UB_INC		0x04	/* upper bound is inclusive */
;; #define RANGE_LB_INF		0x08	/* lower bound is -infinity */
;; #define RANGE_UB_INF		0x10	/* upper bound is +infinity */
;; #define RANGE_LB_NULL		0x20	/* lower bound is null (NOT USED) */
;; #define RANGE_UB_NULL		0x40	/* upper bound is null (NOT USED) */
;; #define RANGE_CONTAIN_EMPTY 0x80	/* marks a GiST internal-page entry whose
;; 									 * subtree contains some empty ranges */

(defn binary-copy-table-writers [colnames copy!]
  (let [a (byte-array (* 1024 1024))
        bb (doto (java.nio.ByteBuffer/wrap a)
             (.order java.nio.ByteOrder/BIG_ENDIAN)
             .mark)
        pg-epoch (.getTime ^java.util.Date (instant/read-instant-date "2000-01-01"))
        #_#_conn ^org.postgresql.core.BaseConnection (mk-conn)
        #_#_sql-copy (print-str "COPY" (sql-ident (name (case table-name
                                                          :line-item "line-item:0"
                                                          :line-item-details "line-item-details:0"
                                                          table-name))) "(" (str/join ", " (map sql-ident colnames)) ") FROM STDIN (FORMAT binary)")
        #_#_out (-> conn org.postgresql.copy.CopyManager.
                  (.copyIn sql-copy))
        flush-buf! #(doto bb
                      (.limit (.position bb))
                      .reset
                      (->> .position (java.util.Arrays/copyOfRange a 0) copy!)
                      .compact)
        ncols (count colnames)]
    {:init-table      (fn [] bb)
     :init-tuple      (fn [_] (when (< (.remaining bb) 2) (flush-buf!)) (.putShort bb ncols))
     :terminate-tuple (fn [t _] (.mark bb) t)
     :terminate-table (fn [_] (flush-buf!))
     :write-text      (fn [_ ^String s]
                        (when (< (.remaining bb) 4) (flush-buf!))
                        (if (nil? s)
                          (doto bb (.putInt -1))
                          (let [ba (.getBytes s java.nio.charset.StandardCharsets/UTF_8)
                                n (alength ba)]
                            (.putInt bb n)
                            (loop [i 0]
                              (let [r (.remaining bb)
                                    m (- n i)]
                                (if (< r m)
                                  (do
                                    (.put bb ba i r)
                                    (flush-buf!)
                                    (recur (+ i r)))
                                  (.put bb ba i m)))))))
     :write-double    (fn [_ ^Double d]
                        (when (< (.remaining bb) 12) (flush-buf!))
                        (if (nil? d)
                          (doto bb (.putInt -1))
                          (doto bb (.putInt 8) (.putDouble d))))
     :write-int       (fn [_ ^Integer n]
                        (when (< (.remaining bb) 8) (flush-buf!))
                        (if (nil? n)
                          (doto bb (.putInt -1))
                          (doto bb (.putInt 4) (.putInt n))))
     :write-timestamp (fn [_ ^String t]
                        (when (< (.remaining bb) 12) (flush-buf!))
                        (if (nil? t)
                          (doto bb (.putInt -1))
                          (doto bb
                            (.putInt 8)
                            (.putLong (* 1000 (- (.getTime ^java.util.Date (instant/read-instant-date t))
                                                pg-epoch))))))
     :write-range     (fn [_ from to]
                        (when (< (.remaining bb) 29) (flush-buf!))
                        (doto bb
                          (.putInt 25)
                          (.put (unchecked-byte 2))         ; flags = lower bound included
                          (.putInt 8)
                          (.putLong (* 1000 (- (.getTime ^java.util.Date (instant/read-instant-date from))
                                              pg-epoch)))
                          (.putInt 8)
                          (.putLong (* 1000 (- (.getTime ^java.util.Date (instant/read-instant-date to))
                                              pg-epoch)))))}))

(def PGCOPY-START
  (byte-array (concat (map int "PGCOPY\n\377\r\n\0") (repeat 8 0))))

(def PGCOPY-END
  (byte-array [-1 -1]))

(defn threadq
  [{:keys [handler mk-conn envf donef ^String thread-name]
    :or   {donef       identity
           envf        (constantly {})
           thread-name "aws-cur:ingest"}}]
  (let [*open? (atom true)
        q (java.util.concurrent.SynchronousQueue.)
        result-promise (promise)
        thread (doto (Thread.
                       (fn []
                         (try
                           (with-open [conn ^org.postgresql.core.BaseConnection (mk-conn)]
                             (let [base-env {:conn conn}
                                   env (merge base-env (envf base-env))]
                               (loop []
                                 (let [x (.take q)]
                                   (when-not (identical? q x) ; EOQ
                                     (handler env x)
                                     (recur))))
                               (donef env)
                               (deliver result-promise true)))
                           (catch Throwable ex
                             (deliver result-promise ex))
                           (finally
                             (reset! *open? false))))
                       thread-name)
                 .start)]
    (fn
      ([]
       (when (compare-and-set! *open? true false)
         (.put q q))
       (let [r @result-promise]
         (if (instance? Throwable r)
           (throw r)
           r)))
      ([x]
       ;; retry forever for now
       (loop []
         (and @*open?
           (or (.offer q x 500 java.util.concurrent.TimeUnit/MILLISECONDS)
             (recur))))))))

(comment
  (def get-conn
    (postgres/connection-factory {:db-spec {:host   "localhost"
                                            :dbname "csp_billing"
                                            :user   "postgres"}}))
  (def tq (threadq {:handler (fn [_ msg]
                               (prn "start" msg)
                               (when (= "ex" msg)
                                 (throw (ex-info "failure" {:msg msg})))
                               (Thread/sleep 1000)
                               (prn "done" msg))
                    :mk-conn get-conn}))
  (tq "a")
  (tq "ex")
  (tq))

(defn copy-fn
  [mk-conn table cols]
  (threadq
    {:mk-conn     mk-conn
     :envf        (fn [{:keys [conn] :as env}]
                    (let [sql-copy (print-str "COPY" (sql-ident table)
                                     "(" (str/join ", " (map sql-ident cols)) ") FROM STDIN (FORMAT binary)")
                          out (-> conn org.postgresql.copy.CopyManager.
                                (.copyIn sql-copy))]
                      (.writeToCopy out PGCOPY-START 0 (alength ^bytes PGCOPY-START))
                      (assoc env :out out)))
     :handler     (fn copy-fn-handler
                    [{:keys [^org.postgresql.copy.CopyIn out]} x]
                    (.writeToCopy out ^bytes x 0 (alength ^bytes x)))
     :donef       (fn [{:keys [^org.postgresql.copy.CopyIn out]}]
                    (.writeToCopy out PGCOPY-END 0 (alength ^bytes PGCOPY-END))
                    (dbg-prn "ENDING COPY" table)
                    (.endCopy out))
     :thread-name (str "COPY TO " (sql-ident table))}))

(defn one-shot-conn-fn
  "Takes a connection-returning thunk, creates a connection and spawns a thread owning this connection.
   Returns a function with two arities:
   [] -> close the connection and terminates the thread
   [f & args] -> execute (apply f conn args) on the connection thread, returns the return value to the calling thread."
  [mk-conn]
  (let [qf (threadq
             {:mk-conn     mk-conn
              :envf        (fn [{:keys [^java.sql.Connection conn] :as env}]
                             (.setAutoCommit conn true)
                             env)
              :handler     (fn one-shot-conn-fn-handler
                             [{:keys [conn]} x]
                             (x conn))
              :thread-name "CONN 1-shot"})]
    ;; Override qf impl to return message result to calling thread via promise
    (fn
      ([] (qf))
      ([f & args]
       (let [p (promise)
             ;; fn to get called in :handler
             f (fn [conn]
                 (deliver p (try
                              (apply f conn args)
                              (catch Exception ex ex))))
             ;; put on queue
             _ (qf f)
             result @p]
         (when (instance? Throwable result) (throw result))
         result)))))

#_(defn assert-table-spec [{:keys [cols key kvs partition-by global-id] whensome :when-some}]
    ())

(defn copy-conns-for-table [[table-name {:keys [cols key kvs sub-partition-by global-id ref-data] whensome :when-some}] mk-conn]
  (let [table-name (name table-name)
        partitioned-by-bill (not (or #_global-id ref-data))
        suffixed-table-name (cond-> table-name partitioned-by-bill (str *import-suffix*))]
    {:main (copy-fn mk-conn suffixed-table-name (cond->> (map (comp first col-spec) cols) (seq key) (cons "id")))
     :kvs  (when kvs (copy-fn mk-conn (str suffixed-table-name ":kv") (if ref-data ["id" "k" "v"] ["bill/id" "id" "k" "v"])))
     :flush-whensome
     (when whensome
       (copy-fn mk-conn suffixed-table-name (cons "id" key)))}))


(defn flush-ids [ids table-writers flush-copy]
  (dbg-prn "FLUSH" (count (filter odd? (vals ids))) (count ids))
  (let [{:keys [init-table terminate-table init-tuple terminate-tuple
                write-int write-text] :as table-writer}
        (table-writers ["id" "k"] flush-copy)
        state (init-table)
        state (reduce (fn [acc [k id]]
                        (if (odd? id)
                          (let [tuple-acc
                                (-> acc
                                  init-tuple
                                  (write-int (quot id 2))
                                  (write-text (csv/single-col-key-str k)))]
                            (terminate-tuple acc tuple-acc))
                          acc)) state ids)]
    (terminate-table state)))

(defn create-id-atom-lookup!
  "Returns a map keyed by table name mapped to an atom."
  [^java.sql.Connection conn csv-structure]
  (let [stmt (.createStatement conn)]
    (into {}
      (keep (fn [[k v]]
              (when (seq (:key v))
                [k (atom
                     (if (:ref-data v)
                       (let [add-state (if (:when-some v) #(* 2 %) identity)]
                         (into {}
                           (map (fn [{:keys [id k]}]
                                  [(csv/string-key java.nio.charset.StandardCharsets/UTF_8 (str k)) (add-state id)]))
                           (resultset-seq
                             (.executeQuery stmt (str "SELECT id, " (sql-ident (first (:key v))) " AS k FROM " (sql-ident (name k)))))))
                       {}))])))
      csv-structure)))

(defn get-interned-string-lookup
  "Returns a map of existing interned string text to the interned string id."
  [^java.sql.Connection conn]
  (let [stmt (.createStatement conn)]
    (into {}
      (map (fn [{:keys [id text]}]
             (let [text (if (neg? (.indexOf ^String text ","))
                          text
                          (str \" text \"))]
               [(csv/string-key java.nio.charset.StandardCharsets/UTF_8 text) id])))
      (resultset-seq
        (.executeQuery stmt "SELECT text, id FROM \"interned-string\"")))))

(defn import-fn
  ([csv-structure] (import-fn csv-structure debug-table-writer nil))
  ([csv-structure table-writers mk-conn]
   (when-not (bound? #'*import-suffix*)
     (throw (IllegalStateException. "*import-suffix* must be set.")))
   (let [table-names
         (topo-sort
           (into {}
             (map (fn [[table spec]]
                    [table (into #{}
                             (keep #(let [[_ op ref] (col-spec %)] (case op :fk ref nil)))
                             (:cols spec))]))
             csv-structure))
         in-topo-order-table-specs (map #(find csv-structure %) table-names)
         sql-exec (one-shot-conn-fn mk-conn)
         #_#_sql-execs-pool (repeatedly 8 #(one-shot-conn-fn mk-conn))
         ddls (concat
                (mapcat #(ddls % *import-suffix*)
                  (cons [:interned-string {:ref-data    true
                                           :key         ["text"]
                                           :cols        [["text" :text]]
                                           :indexes     {"covering-auto" {:method  :btree
                                                                          :cols    ["id"]
                                                                          :include ["text"]}}
                                           :custom-ddls [#_{:post "CREATE INDEX IF NOT EXISTS \"interned-string:covering\" ON \"interned-string\" (id) INCLUDE (text)"}]}]
                    in-topo-order-table-specs))
                (summary-tables-ddls
                  "costs"
                  summary-tables-structure))
         ;; Execute all :pre DDLs
         _ (sql-exec
             (fn [^java.sql.Connection conn]
               (let [stmt (.createStatement conn)]
                 (doseq [{ddl :pre} ddls
                         :when ddl]
                   (dbg-prn :pre ddl)
                   (.execute stmt ddl)))))
         ids-atoms (sql-exec #(create-id-atom-lookup! % csv-structure))
         interns (sql-exec get-interned-string-lookup)
         copies (into {}
                  (map (fn [[table-name :as table-name+table-spec]]
                         [table-name (copy-conns-for-table table-name+table-spec mk-conn)]))
                  (cons [:interned-string {:ref-data true
                                           :key      ["dummy"]
                                           :cols     [["text" :text]]}]
                    in-topo-order-table-specs))
         env {:interns  (atom interns)
              :ids      ids-atoms
              :ddls     ddls
              :sql-exec sql-exec
              :tables   {}
              :copies   copies}]
     {:env env
      :import-fn
      (fn
        ([]
         (doseq [[table m] copies
                 [tag copy] m
                 :when copy]
           (dbg-prn "TERM" tag table)
           (when (= tag :flush-whensome)
             (dbg-prn "flush")
             (flush-ids @(ids-atoms table) table-writers copy))
           (copy))
         (sql-exec
           (fn [^java.sql.Connection conn]
             (let [stmt (.createStatement conn)]
               (doseq [{:keys [post]} ddls]
                 (when post
                   (doseq [ddl (cond-> post (string? post) vector)
                           ddl (if (coll? ddl) ddl [ddl])]
                     (dbg-prn 'RUN :post ddl)
                     (dbg-time (.execute stmt ddl))))))))
         (sql-exec))
        ([in]
         (with-open [s (csv/scanner in)]
           (csv/read-headers s)
           (let [env (assoc env :scanner s)
                 env (assoc env :intern-table (compile-intern env table-writers))
                 env
                 (reduce
                   (fn [env [table-name table-spec]]
                     (try
                       (update env :tables assoc table-name (compile-table-spec env table-name table-spec table-writers))
                       (catch Exception e
                         (throw (ex-info "GOTIT" {:table table-name} e)))))
                   env in-topo-order-table-specs)
                 row (-> env :tables (get (peek table-names)) :row-fn)]
             (while (<= 0 (.seek s))
               (row))
             (let [tables (reduce (fn [tables table-name]
                                    (let [{:keys [terminate] :as table} (tables table-name)
                                          table (-> table (dissoc :terminate)
                                                  (assoc :summary (terminate)))]
                                      (assoc tables table-name table)))
                            (:tables env) table-names)]
               ((-> env :intern-table :terminate))
               (assoc (dissoc env :intern-table) :tables tables))))))})))

(comment (sc.api/defsc 1))

(defn dir->input-stream-fns
  [{::keys [dir]}]
  (->> (file-seq (io/file dir))
    (filter (fn [^File file]
              (str/ends-with? (.getName file) ".gz")))
    (map (fn [^File file]
           (fn []
             {:cs.aws-cur.ingest/name         (.getAbsolutePath file)
              :cs.aws-cur.ingest/input-stream (io/input-stream file)})))))

(defn ensure-import-table [^java.sql.Connection conn]
  (-> conn .createStatement
    (.execute "CREATE TABLE IF NOT EXISTS imports (id text not null, \"cur-id\" text not null, \"auto-id\" serial, state text not null, \"import-start\" timestamp not null default current_timestamp, \"import-end\" timestamp, \"billing-period-start\" timestamp, \"billing-period-end\" timestamp, meta jsonb, primary key(id, \"cur-id\"))")))

(defn tenant-schema
  [tenant-id]
  (str "aws-" tenant-id))

(defn schema-ident
  [tenant-id]
  (-> tenant-id tenant-schema sql-ident))

(s/def ::import-*-argm
  (s/keys :req-un [::tenant-id
                   ::get-conn
                   ::import-id
                   ::cur-id]))

(s/def ::import-cur-argm
  (s/merge ::import-*-argm
    (s/keys :req-un [::approx-date
                     ::input-stream-fns])))

(defn import-cur
  "Takes an import map and uploads data into postgresql.
   Keys of the import map are:
   * :tenant-id, a string identifying the tenant;
   * :approx-date, a yyyy-mm-dd date as string estimating the day of the import data
     -- this impacts how time-partitioned tables are balanced across the month;
   * :input-stream-fns, a collection of InputStream-returning thunks, one per CSV;
   * :get-conn a Connection-retuning thunk;
   * :thread-count (optional) number of input-streams processed concurrently."
  [{:keys [tenant-id approx-date input-stream-fns get-conn thread-count cur-id import-id] :as args}]
  (let [thread-count (min (or thread-count (* (cp/ncpus) 2)) (count input-stream-fns))
        schema-id (schema-ident tenant-id)
        all-conns (atom [])
        mk-conn #(doto ^java.sql.Connection (get-conn)
                   (->> (swap! all-conns conj))
                   (-> .createStatement
                     (doto
                       (.execute (str "CREATE SCHEMA IF NOT EXISTS " schema-id ";"))
                       (.execute (str "SET search_path TO " schema-id ",public;")))))
        import-conn (doto ^java.sql.Connection (mk-conn)
                      ensure-import-table)
        auto-id (-> import-conn .createStatement
                  (.executeQuery (str "INSERT INTO imports(id, \"cur-id\", state) VALUES(" (sql-string import-id) ", " (sql-string cur-id) ", 'importing') RETURNING \"auto-id\""))
                  postgres/str-keyed-resultset-seq
                  first
                  (get "auto-id"))]
    (try
      (binding [*import-suffix* (str "-" auto-id)
                *partitions-count* (count input-stream-fns) ; heuristic
                *approx-date* approx-date]
        (let [{:keys [import-fn env]} (import-fn model.ingest/csv-structure binary-copy-table-writers mk-conn)
              tables (into [] (mapcat :tables) (:ddls env))]
          (try
            (cp/with-shutdown! [pool (cp/threadpool thread-count :name "aws-import-cur")]
              (doall
                (cp/upmap pool
                  (fn [is-fn]
                    (let [{::keys [name input-stream]} (is-fn)]
                      (import-fn (java.util.zip.GZIPInputStream. input-stream))))
                  input-stream-fns)))
            (finally
              (import-fn)))
          (-> import-conn .createStatement
            (.executeQuery (str "UPDATE imports SET state='imported', \"import-end\"=current_timestamp, \"billing-period-start\"=b.\"bill/BillingPeriodStartDate\", \"billing-period-end\"=b.\"bill/BillingPeriodEndDate\" FROM (SELECT * from " (sql-ident "bill" *import-suffix*) "LIMIT 1) b WHERE imports.id=" (sql-string import-id) " AND \"cur-id\"=" (sql-string cur-id) " AND state='importing' RETURNING \"billing-period-start\", \"billing-period-end\""))
            postgres/str-keyed-resultset-seq
            first)
          {:tables tables}))
      (catch Exception e
        (-> import-conn .createStatement
          (.executeUpdate (str "UPDATE imports SET state='failed', \"import-end\"=current_timestamp, meta=coalesce(meta, '{}'::jsonb) || jsonb_build_object('cause', " (sql-string (pr-str e)) ") WHERE id=" (sql-string import-id) " AND \"cur-id\"=" (sql-string cur-id) " AND state='importing'")))
        (throw e))
      (finally
        (run! #(.close ^java.lang.AutoCloseable %) @all-conns)))))

(s/fdef import-cur
  :args (s/cat :argm ::import-cur-argm))

(defn analyze-import
  [{:keys [tenant-id get-conn import-cur] :as args}]
  (let [{:keys [tables]} import-cur
        schema-id (tenant-schema tenant-id)]
    (with-open [conn ^java.sql.Connection (get-conn)]
      (postgres/set-schema! conn {:schema schema-id})
      (-> conn
        (.createStatement)
        (.executeUpdate (str "ANALYZE " (str/join ", " (map sql-ident (sort tables)))))))))

(s/def ::attach-import-argm ::import-*-argm)

(defn- get-auto-id [^java.sql.Statement stmt cur-id import-id]
  (-> stmt
    (.executeQuery (str "select \"auto-id\" from imports where id=" (sql-string import-id) " and \"cur-id\"=" (sql-string cur-id)))
    postgres/str-keyed-resultset-seq
    first
    (get "auto-id")
    (some->> (str "-"))))

(defn attach-import
  "Prefer using supersede-import."
  ([{:keys [tenant-id get-conn import-id cur-id] :as args}]
   (let [schema-id (schema-ident tenant-id)]
     (with-open [conn (doto ^java.sql.Connection (get-conn)
                        (-> .createStatement
                          (doto
                            (.execute (str "CREATE SCHEMA IF NOT EXISTS " schema-id ";"))
                            (.execute (str "SET search_path TO " schema-id ",public;")))))]
       (attach-import import-id cur-id conn))))
  ([import-id cur-id ^java.sql.Connection conn]
   (with-open [stmt (.createStatement conn)]
     (let [import-suffix (get-auto-id stmt cur-id import-id)
           bill-ids
           (str/join ", " (map :id (resultset-seq (.executeQuery stmt (str "SELECT id FROM " (sql-ident "bill" import-suffix))))))]
       (doseq [[table {:keys [ref-data kvs]}] model.ingest/csv-structure
               :when (not (or #_global-id ref-data))        ; partitioned-by-bill
               suffix (cond-> [""] kvs (conj ":kv"))]
         (.execute stmt (doto (str "ALTER TABLE " (sql-ident (name table) suffix)
                                " ATTACH PARTITION " (sql-ident (name table) import-suffix suffix)
                                " FOR VALUES IN (" bill-ids ")")
                          dbg-prn)))
       (doseq [period [:hourly :daily :monthly]]
         (.execute stmt (doto (str "ALTER TABLE " (sql-ident "costs:" (name period))
                                " ATTACH PARTITION " (sql-ident "costs" import-suffix ":" (name period))
                                " FOR VALUES IN (" bill-ids ")")
                          dbg-prn)))))))

(s/fdef attach-import
  :args (s/cat :argm ::attach-import-argm))

(s/def ::detach-import-argm ::import-*-argm)

(defn detach-import
  "Prefer using supersede-import."
  ([{:keys [tenant-id get-conn import-id cur-id] :as args}]
   (let [schema-id (schema-ident tenant-id)]
     (with-open [conn (doto ^java.sql.Connection (get-conn)
                        (-> .createStatement
                          (doto
                            (.execute (str "CREATE SCHEMA IF NOT EXISTS " schema-id ";"))
                            (.execute (str "SET search_path TO " schema-id ",public;")))))]
       (detach-import import-id cur-id conn))))
  ([import-id cur-id ^java.sql.Connection conn]
   (with-open [stmt (.createStatement conn)]
     (let [import-suffix (get-auto-id stmt cur-id import-id)]
       (doseq [[table {:keys [ref-data kvs]}] model.ingest/csv-structure
               :when (not (or #_global-id ref-data))        ; partitioned-by-bill
               suffix (cond-> [""] kvs (conj ":kv"))]
         (.execute stmt (doto (str "ALTER TABLE " (sql-ident (name table) suffix)
                                " DETACH PARTITION " (sql-ident (name table) import-suffix suffix))
                          dbg-prn)))
       (doseq [period [:hourly :daily :monthly]]
         (.execute stmt (doto (str "ALTER TABLE " (sql-ident "costs:" (name period))
                                " DETACH PARTITION " (sql-ident "costs" import-suffix ":" (name period)))
                          dbg-prn)))))))

(s/fdef detach-import
  :args (s/cat :argm ::detach-import-argm))

(s/def ::supersede-import-argm ::import-*-argm)

(defn supersede-import
  "Takes an import map (import-id, cur-id, tenant-id and get-conn) and attach the import (if imported)
   and detach currently attached imports for the same billing period.
   Returns a collection of detached import-ids.
   Should be the preferred way to attach/detach imports."
  [new-import]
  (let [{:keys [tenant-id get-conn import-id cur-id]} new-import
        schema-id (schema-ident tenant-id)]
    (with-open [conn (doto ^java.sql.Connection (get-conn)
                       (-> .createStatement
                         (doto
                           (.execute (str "CREATE SCHEMA IF NOT EXISTS " schema-id ";"))
                           (.execute (str "SET search_path TO " schema-id ",public;"))))
                       ensure-import-table
                       (.setAutoCommit false))]
      (let [stmt (.createStatement conn)
            detached-ids
            (into [] (map :id)
              (resultset-seq
                (.executeQuery stmt (str "UPDATE imports o SET state='imported', meta=coalesce(o.meta, '{}'::jsonb) || jsonb_build_object('detached-on', current_timestamp) FROM imports a WHERE o.state='attached' AND a.id=" (sql-string import-id) " AND a.\"cur-id\"=" (sql-string cur-id) " AND a.state='imported' AND a.\"cur-id\"=o.\"cur-id\" AND a.\"billing-period-start\"=o.\"billing-period-start\" AND a.\"billing-period-end\"=o.\"billing-period-end\" RETURNING o.id"))))]
        (.execute stmt (str "UPDATE imports SET state='attached', meta=coalesce(meta, '{}'::jsonb) || jsonb_build_object('attached-on', current_timestamp) WHERE state='imported' AND id=" (sql-string import-id) " AND \"cur-id\"=" (sql-string cur-id)))
        (run! #(detach-import % cur-id conn) detached-ids)
        (attach-import import-id cur-id conn)
        (.commit conn)
        detached-ids))))

(s/fdef supersede-import
  :args (s/cat :argm ::supersede-import-argm))

(s/def ::drop-import-argm ::import-*-argm)

(defn drop-import
  "Remove data associated to the import from the database.
   Metadata about the import IS NOT deleted thus you can't reuse the import-id.
   Products, pricing and interned strings introduced by this import are not deleted
   as they may be used by later import."
  [{:keys [tenant-id get-conn import-id cur-id] :as args}]
  (let [schema-id (schema-ident tenant-id)]
    (with-open [conn (doto ^java.sql.Connection (get-conn)
                       (.setAutoCommit false)
                       (-> .createStatement
                         (doto
                           (.execute (str "CREATE SCHEMA IF NOT EXISTS " schema-id ";"))
                           (.execute (str "SET search_path TO " schema-id ",public;")))))
                stmt (.createStatement conn)]
      (let [import-suffix (get-auto-id stmt cur-id import-id)]
        (doseq [[table {:keys [ref-data kvs]}] model.ingest/csv-structure
                :when (not (or #_global-id ref-data))       ; partitioned-by-bill
                suffix (cond-> [""] kvs (conj ":kv"))]
          (.execute stmt (doto (str "DROP TABLE IF EXISTS " (sql-ident (name table) import-suffix suffix))
                           dbg-prn)))
        (doseq [period [:hourly :daily :monthly]]
          (.execute stmt (doto (str "DROP TABLE IF EXISTS " (sql-ident "costs" import-suffix ":" (name period)))
                           dbg-prn))))
      (.executeUpdate stmt (str "UPDATE imports SET state='dropped', meta=coalesce(meta, '{}'::jsonb) || jsonb_build_object('dropped-on', current_timestamp) WHERE id=" (sql-string import-id) " AND \"cur-id\"=" (sql-string cur-id)))
      (.commit conn))))

(s/fdef drop-import
  :args (s/cat :argm ::drop-import-argm))

(comment
  (def base-argm
    {:tenant-id   "cs2"
     :approx-date "2022-01-31"
     :get-conn    (postgres/connection-factory {:db-spec {:host   "localhost"
                                                          :dbname "cur-cs"}})
     :cur-id      "cs-hourly"
     #_#_:conn-opts {}
     #_#_:tag "something-to-suffix-if-you-perform-several-imports-for-a-given-date"})

  (def baptiste-argm
    {:tenant-id "cust1"
     :as-of     "2021-08-31"
     :get-conn  (postgres/connection-factory
                  {:db-spec {:host     "curdb.cnoppxoie9fj.eu-west-1.rds.amazonaws.com"
                             :dbname   "CUR"
                             :user     "postgres"
                             :password "Baptiste777"}})
     #_#_:conn-opts {}
     #_#_:tag "something-to-suffix-if-you-perform-several-imports-for-a-given-date"}))

(comment
  ; Step #1: import data
  ; takes around 25 minutes on my MBP2019 i7 6cores/12HT
  ; first half is really importing
  ; second half is the db building indexes and aggregated tables
  (time (import-cur (assoc base-argm
                      :input-stream-fns (dir->input-stream-fns {::dir "../../CUR/BIG/"}))))

  (time (import-cur
          (assoc base-argm
            :input-stream-fns (take 1 (dir->input-stream-fns {::dir "../../CUR/BIG/"}))
            :tag "partial-too2")))

  (time (import-cur
          (assoc base-argm
            :input-stream-fns (dir->input-stream-fns {::dir "../../CUR/BUG/"})
            :import-id "main2")))

  (time (supersede-import
          (assoc base-argm
            :import-id "main2")))

  (time (supersede-import
          base-argm
          (assoc base-argm
            :tag "partial")))

  (time (supersede-import
          (assoc base-argm
            :tag "partial")
          base-argm))

  ;; dev machine psql -U postgres -W -F p -h curdb.cnoppxoie9fj.eu-west-1.rds.amazonaws.com
  (time (import-cur (assoc baptiste-argm
                      :input-stream-fns (dir->input-stream-fns {::dir "../../BIG/"}))))

  ; Step #2 brings the data online (argument map is the same at all steps)
  (attach-import base-argm)

  (attach-import baptiste-argm)

  ; Bring a CUR offline -- generally because it has been superseded by a CUR
  (detach-import base-argm)

  ; Delete data for a CUR
  (drop-import (assoc base-argm :tag "avec-import6"))
  (supersede-import (assoc base-argm :tag "avec-import9"))

  )
