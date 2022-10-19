(ns computesoftware.aws-cur.ingest.model)

(def resource-tag-re
  "Regex matching resouce tag columns from a CUR."
  #"resourceTags/(.*)")

(def cost-category-re
  "Regex matching cost category columns from a CUR."
  #"costCategory/(.*)")

(def tag-or-cost-category-re
  (re-pattern (str resource-tag-re "|" cost-category-re)))

(def csv-structure
  "This structure controls how input CSV files are renormalized and inserted into pg.
   Its very adhoc to this project.
   Top-level structure is a map of table names (as keywords) to table-specs.
   A table-spec is a map which may have the following keys:
   * :global-id (boolean) means that the surrogate key is allocated through the db
     (and not locally)
   * :key a vector of csv columns names (as strings)
   * :cols a vector of col-specs
   * :when-some (regexp) a row will be written only if there's at least one
     non-empty column whose header matches this regexp -- the surrogate id
     is still allocated; it's used for products where details are not always present,
     it allows to wait for details to be flushed by a later csv line.
   * :ref-data (boolean) non-partitioned table (when true), its data won't be reclaimed
     by drop-import;
     when false (default) the table is partitioned by bill/id
   * :kvs (regexp) all columns whose header matches this regexp are stored in a kv-table,
     and all values are interned strings.
   * :sub-partition-by (pair [op col] where op is either :hash or :range
      and col a csv column name as string) triggers a second level of partitioning
      either hash-based or time-based.
   A col-spec is either:
   * a string meaning the sql column and the csv column have the same name and that
     the value is an interned string
   * [sql-colname-as-string type & args] where type can be:
     * :text, one optional argument: the csv column name if different from the sql one
     * :numerci, one optional argument: the csv column name if different from the sql one
     * :datetime, first optional argument the csv column name, second optional argument
                  :start or :end when the csv column contains ranges
     * :fk with one argument the target table (as keyword)"
  {:bill              {:global-id true
                       :key       ["bill/BillType"
                                   "bill/BillingEntity"
                                   "bill/BillingPeriodEndDate"
                                   "bill/BillingPeriodStartDate"
                                   "bill/InvoiceId"
                                   "bill/PayerAccountId"]
                       :cols      ["bill/BillType"
                                   "bill/BillingEntity"
                                   ["bill/BillingPeriodStartDate" :datetime]
                                   ["bill/BillingPeriodEndDate" :datetime]
                                   #_["bill/BillingPeriod" :datetime-range "bill/BillingPeriodStartDate" "bill/BillingPeriodEndDate"]
                                   ["bill/InvoiceId" :text]
                                   "bill/PayerAccountId"]}
   :product           {:ref-data  true
                       :key       ["product/sku"]
                       :cols      [["product/sku" :text]]
                       :when-some #"product/(?!(?:ProductName|sku)$).*"
                       :kvs       #"product/(?!(?:ProductName|sku)$).*"}
   :pricing           {:ref-data true
                       :key      ["pricing/RateId"]
                       :cols     [["pricing/RateId" :text]
                                  "pricing/currency"
                                  ["pricing/publicOnDemandRate" :numeric]
                                  "pricing/term"
                                  "pricing/unit"]}
   :line-item-attributes
   {:key  [; bill/id
           "bill/BillType"
           "bill/BillingEntity"
           "bill/BillingPeriodEndDate"
           "bill/BillingPeriodStartDate"
           "bill/InvoiceId"
           "bill/PayerAccountId"
           ; attributes
           "lineItem/LineItemDescription"
           "lineItem/LineItemType"
           "lineItem/ProductCode"
           "lineItem/UsageAccountId"
           "lineItem/UsageType"
           "lineItem/BlendedRate"
           "lineItem/LegalEntity"
           "lineItem/Operation"
           "lineItem/UnblendedRate"
           "product/ProductName"
           "product/sku"                                    ;* key of fk
           "pricing/RateCode"
           "pricing/RateId"                                 ;* key of fk
           tag-or-cost-category-re]
    :cols [["bill/id" :fk :bill]
           "lineItem/LineItemDescription"
           "lineItem/LineItemType"
           "lineItem/ProductCode"
           "lineItem/UsageAccountId"
           "lineItem/UsageType"
           ["lineItem/BlendedRate" :numeric]
           "lineItem/LegalEntity"
           "lineItem/Operation"
           ["lineItem/UnblendedRate" :numeric]
           "product/ProductName"
           ["product/id" :fk :product]
           "pricing/RateCode"
           ["pricing/id" :fk :pricing]]
    :kvs  tag-or-cost-category-re}
   :line-item         {:key              ["identity/LineItemId"]
                       :sub-partition-by [:hash "id"]
                       :indexes          {"resource-id" {:method :btree
                                                         :cols   ["lineItem/ResourceId"]}}
                       :cols             [["bill/id" :fk :bill]
                                          ["identity/LineItemId" :text]
                                          ["lineItem/ResourceId" :text]
                                          ["line-item-attributes/id" :fk :line-item-attributes]]}
   :line-item-details {:sub-partition-by [:range "identity/TimeIntervalStart"]
                       :cols             [["bill/id" :fk :bill]
                                          ["lineItem/id" :fk :line-item]
                                          #_["identity/TimeInterval" :datetime-range]
                                          ["identity/TimeIntervalStart" :datetime "identity/TimeInterval" :start]
                                          ["identity/TimeIntervalEnd" :datetime "identity/TimeInterval" :end]
                                          ["lineItem/BlendedCost" :numeric]
                                          ["lineItem/UnblendedCost" :numeric]
                                          ["lineItem/UsageAmount" :numeric]
                                          #_["lineItem/UsagePeriod" :datetime-range "lineItem/UsageStartDate" "lineItem/UsageEndDate"]
                                          ["lineItem/UsageStartDate" :datetime]
                                          ["lineItem/UsageEndDate" :datetime]
                                          ["pricing/publicOnDemandCost" :numeric]
                                          ["lineItem/NormalizedUsageAmount" :numeric]
                                          ["savingsPlan/SavingsPlanEffectiveCost" :numeric]
                                          ["savingsPlan/UsedCommitment" :numeric]
                                          ["reservation/AmortizedUpfrontCostForUsage" :numeric]
                                          ["reservation/EffectiveCost" :numeric]
                                          ["reservation/RecurringFeeForUsage" :numeric]]}})
