(ns computesoftware.aws-cur.ingest-test
  (:require
    [clojure.java.io :as io]
    [clojure.set :as sets]
    [clojure.spec.alpha :as s]
    [clojure.test :refer :all]
    [clojure.test.check.clojure-test :as tc.clojure-test]
    [clojure.test.check.generators :as gen]
    [clojure.test.check.properties :as prop]
    [computesoftware.aws-cur.ingest :as ingest]
    [computesoftware.aws-cur.ingest.model :as model.ingest]
    [computesoftware.aws-cur.query :as query]
    [computesoftware.postgres :as postgres]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]
    [provisdom.test.core :as t]))

(def db-spec {:dbname "csp_billing", :host "localhost", :user "postgres"})
(def get-conn (postgres/connection-factory {:db-spec db-spec}))

(defn get-big-cur-input-stream-fns
  []
  (into []
    (comp
      (filter (memfn isFile))
      (map (fn [f]
             (fn []
               {::ingest/name         (.getName f)
                ::ingest/input-stream (io/input-stream f)}))))
    (file-seq (io/file "test-data/big-cur"))))

(def example-argm
  {:tenant-id   "cust1"
   :approx-date "2021-08-31"
   :cur-id      "default"})

(deftest ingest-test
  (let [args (assoc example-argm
               :input-stream-fns (take 2 (get-big-cur-input-stream-fns))
               :get-conn get-conn)
        schema-id (ingest/schema-ident (:tenant-id args))
        arg-day1 (assoc args :import-id "day1")
        arg-day2 (assoc args :import-id "day2")
        count-line-items "SELECT count(*) n FROM \"line-item\""
        count-line-items-day1 "SELECT count(*) n FROM \"line-item-1\"" ; hard coding a serially attributed value
        attached-imports "SELECT id, \"cur-id\" FROM imports WHERE state='attached'"
        count-interns "SELECT count(*) n FROM \"interned-string\""
        no-dups "select distinct * from (select count(id) n from \"interned-string\" group by text) _"
        count-products "SELECT count(*) n FROM product"
        count-product-kvs "SELECT count(*) n FROM \"product:kv\""
        no-dup-products "select distinct * from (select count(id) n from product group by \"product/sku\") _"
        count-pricings "SELECT count(*) n FROM pricing"
        no-dup-pricings "select distinct * from (select count(id) n from pricing group by \"pricing/RateId\") _"]
    (with-open [conn (get-conn)
                stmt (doto (.createStatement conn)
                       (.execute (str "DROP SCHEMA IF EXISTS " schema-id " CASCADE;"))
                       (.execute (str "CREATE SCHEMA IF NOT EXISTS " schema-id ";"))
                       (.execute (str "SET search_path TO " schema-id ",public;")))]
      (let [q #(resultset-seq (.executeQuery stmt %))]
        (testing "import day1"
          (ingest/import-cur arg-day1)
          (testing "interns populated and no duplicates"
            (is (= [{:n 17967}] (q count-interns)))
            (is (= [{:n 1}] (q no-dups))))
          (testing "products populated and no duplicates"
            (is (= [{:n 3027}] (q count-products)))
            (is (= [{:n 49474}] (q count-product-kvs)))
            (is (= [{:n 1}] (q no-dup-products))))
          (testing "pricings populated and no duplicates"
            (is (= [{:n 3232}] (q count-pricings)))
            (is (= [{:n 1}] (q no-dup-pricings))))
          (testing "empty main table, but populated partition"
            (is (= [{:n 0}] (q count-line-items)))
            (is (= [{:n 710437}] (q count-line-items-day1)))))
        (testing "attach day1 import with supersede, now main table populated"
          (ingest/supersede-import arg-day1)                ; not idempotent
          (is (= [{:n 710437}] (q count-line-items)))
          (is (= [{:id "day1", :cur-id "default"}] (q attached-imports))))
        (testing "import same data as day2, check no new interns, products or pricings"
          (ingest/import-cur arg-day2)
          (is (= [{:n 17967}] (q count-interns)))
          (is (= [{:n 1}] (q no-dups)))
          (is (= [{:n 3027}] (q count-products)))
          (is (= [{:n 49474}] (q count-product-kvs)))
          (is (= [{:n 1}] (q no-dup-products)))
          (is (= [{:n 3232}] (q count-pricings)))
          (is (= [{:n 1}] (q no-dup-pricings))))
        (testing "attach day2 with supersede, only day2 is attached now."
          (ingest/supersede-import arg-day2)
          (is (= [{:id "day2", :cur-id "default"}] (q attached-imports)))
          (is (= [{:n 710437}] (q count-line-items))))))))

(def ingest-cols
  (for [[_ {:keys [cols]}] model.ingest/csv-structure
        col cols
        :let [[col col-type] (if (sequential? col) col [col])]
        :when (and (not (contains? #{:fk} col-type))
                ;; Unsupported group bys
                (not (contains? #{"lineItem/ResourceId"
                                  "identity/LineItemId"} col)))]
    (cond-> {:col col}
      col-type (assoc :type col-type))))

(s/def ::cost-map (s/map-of string? any?))
(s/def ::query-ret (s/coll-of ::cost-map))

(defn fetch-col-options
  [conn {:keys [cols]}]
  (jdbc/with-transaction [tx conn]
    (postgres/set-schema! tx {:schema (ingest/tenant-schema (:tenant-id example-argm))})
    (into []
      (map (fn [col]
             (let [sql-params (query/filter-options-sql-params
                                {:start           #inst"2021-08-01"
                                 :stop            #inst"2021-08-03"
                                 :granularity     :daily
                                 :filter-category col})

                   results (jdbc/execute! tx sql-params
                             {:builder-fn rs/as-unqualified-maps})]
               {:col            col
                :filter-options results})))
      cols)))

;; fetch-col-options can be a bit slow, so we store in memory for fast development iterations
(defonce fetch-col-options$ (memoize fetch-col-options))

(def static-cols
  "Cols that we statically define during CUR ingestion."
  (into []
    (comp
      (filter (fn [{:keys [type]}] (contains? #{nil} type)))
      (map :col))
    ingest-cols))

(defn get-dynamic-cols
  "Dynamic cols that depend on the customer's data."
  [conn]
  (jdbc/with-transaction [tx conn]
    (postgres/set-schema! tx {:schema (ingest/tenant-schema (:tenant-id example-argm))})
    (let [is-like-exec #(jdbc/execute! tx [(str "SELECT \"text\" FROM \"interned-string\" WHERE \"text\" LIKE "
                                             (postgres/sql-constant %))])
          product-cols (map :interned-string/text
                         (is-like-exec "product/%"))
          tag-cols (map :interned-string/text
                     (is-like-exec "resourceTags/%"))
          cost-cat-cols (map :interned-string/text
                          (is-like-exec "costCategory/%"))]
      (concat product-cols tag-cols cost-cat-cols))))

(defn get-all-cols
  [conn]
  (concat static-cols (get-dynamic-cols conn)))

(deftest query-test
  (with-open [conn (get-conn)]
    (let [qmap {:granularity :daily
                :from        #inst "2021-08-01"
                :to          #inst "2021-08-03"
                :by          ["product/region"]
                :aggregate   {"lineItem/UnblendedCost" :sum}
                :excluding   {"lineItem/LineItemType" #{"Credit" "Refund"}}}
          ctx (assoc example-argm :conn conn)
          cols (get-all-cols conn)]
      (testing "group by all available cols"
        (doseq [col cols]
          (let [group-ret (try
                            (query/query (assoc qmap :by [col]) ctx)
                            (catch Exception ex
                              (throw (ex-info (str "Query exception on: " col) {:col col} ex))))]
            (t/is-valid ::query-ret group-ret)))))))

(comment
  (def cols (concat static-cols (get-dynamic-cols conn)))
  (def col-opts
    (fetch-col-options conn {:cols cols})))

(defn date-window-gen
  [{:keys [min-date max-date]}]
  (gen/fmap (fn [xs]
              (mapv #(java.util.Date. %) (sort xs)))
    (gen/vector
      (gen/large-integer* {:min (inst-ms min-date) :max (inst-ms max-date)})
      2)))

(defn qmap-gen
  [{:keys [col-options min-date max-date from to]}]
  (let [col->options (into {}
                       (map (juxt :col (fn [{:keys [filter-options]}] (into #{} (map :category-value/name) filter-options))))
                       col-options)
        set-gen (fn [vs]
                  (if (seq vs)
                    (gen/set (gen/elements vs))
                    (gen/return #{})))]
    (gen/let [granularity (gen/elements [:hourly :daily :monthly])
              [from to] (if (and from to)
                          (gen/return [from to])
                          (date-window-gen {:min-date min-date :max-date max-date}))
              aggregate (gen/map
                          (gen/elements ["lineItem/UnblendedCost" "lineItem/BlendedCost"])
                          (gen/elements [:min :max :sum])
                          {:min-elements 1})
              by (gen/vector-distinct (gen/elements (map :col col-options)))
              excluding-cols (gen/vector-distinct (gen/elements (map :col col-options)))
              excluding-vals (apply gen/tuple
                               (map (fn [col]
                                      (set-gen (get col->options col)))
                                 excluding-cols))
              excluding (gen/return (into {} (map vector excluding-cols excluding-vals)))
              including-cols (gen/vector-distinct (gen/elements (map :col col-options)))
              including-vals (apply gen/tuple
                               (map (fn [col]
                                      (let [;; Don't generate a column value that's already in excluding
                                            vs (sets/difference
                                                 (get col->options col)
                                                 (get excluding col #{}))]
                                        (set-gen vs)))
                                 including-cols))
              including (gen/return (into {} (filter (fn [[_ v]] (seq v))) (map vector including-cols including-vals)))]
      {:granularity granularity
       :from        from
       :to          to
       :aggregate   aggregate
       :by          by
       :excluding   excluding
       :including   including})))

(comment
  (def conn (get-conn))

  (def qmap
    (gen/generate
      (qmap-gen {:col-options col-opts
                 :from        #inst "2021-08-01"
                 :to          #inst "2021-08-03"})))
  (ingest/query
    qmap
    (assoc example-argm :conn conn))

  (ingest/query
    {:granularity :daily
     :from        #inst "2021-08-01T00:00:00Z"
     :to          #inst "2021-08-03T00:00:00Z"
     :by          ["product/region"]
     :aggregate   {"lineItem/UnblendedCost" :max
                   "lineItem/BlendedCost"   :sum}
     :including   {"lineItem/ProductCode" #{"CodeBuild"}}
     :excluding   {"product/ProductName" #{"CodeBuild"}}}
    (assoc example-argm :conn conn)))

#_(tc.clojure-test/defspec query-gen-test
    100
    (let [conn (get-conn)
          min-date #inst "2021-08-01"
          max-date #inst"2021-09-01"
          ctx (assoc example-argm :conn conn)
          cols (concat static-cols (get-dynamic-cols conn))
          col-options (fetch-col-options$ conn {:cols cols})]
      (prop/for-all [qmap (qmap-gen {:col-options col-options
                                     :min-date    min-date
                                     :max-date    max-date})]
        (let [result (try
                       (ingest/query qmap ctx)
                       (catch Exception ex
                         (throw (ex-info "Query failed" {:qmap qmap} ex))))]
          (s/valid? ::query-ret result)))))
