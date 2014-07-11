(ns tpch-cass-ingest.seed
  (:require [clojure.string :as s]
            [qbits.alia :as alia]
            [qbits.hayt :as hayt]
            [clj-time.coerce :as c]
            [tpch-cass-ingest.cassandra-connector :as cass]))
(set! *warn-on-reflection* true)
(defmulti parse (fn [table line] table))
(defmulti perform-insert (fn [table obj] table))

(defmethod perform-insert :default [table obj]
  (hayt/insert table (hayt/values obj)))

(defn perform-batch-insert [table lines]
  (alia/execute
    cass/session
    (hayt/batch
      (apply hayt/queries (map #(perform-insert table (parse table %)) lines)))))

(defmethod parse :suppliers [table line]
  (let [[supp-id supp-name address 
         nation-id phone account-balance commnt] (map s/trim (s/split line #"\|"))]
    {:id (Long/parseLong supp-id)
     :name supp-name
     :address address
     :nation_id (Long/parseLong nation-id)
     :phone phone 
     :account_balance (BigDecimal. ^String account-balance)
     :comment commnt}))

(defmethod parse :parts [table line]
  (let [[part-id part-name manufacturer
         brand part-type size container 
         retail-price cmmnt] (map s/trim (s/split line #"\|"))]
    {:id (Long/parseLong part-id)
     :name part-name
     :manufacturer manufacturer
     :brand brand
     :type part-type
     :size (Integer/parseInt size)
     :container container 
     :retail_price (BigDecimal. ^String retail-price)
     :comment cmmnt}))

(defmethod parse :suppliers_parts [table line]
  (let [[part-id supp-id quantity cost] (map s/trim (s/split line #"\|"))]
    {:supplier_id (Long/parseLong supp-id)
     :part_id (Long/parseLong part-id)
     :quantity (Integer/parseInt quantity)
     :cost (BigDecimal. ^String cost)}))

(defmethod perform-insert :supplier-part [table obj]
  (hayt/insert table (hayt/values obj))
  (hayt/update :parts
               (hayt/set-columns :suppliers [+ (first obj)])
               (hayt/where [[= :id (second obj)]]))
  (hayt/update :suppliers
               (hayt/set-columns :parts [+ (second obj)])
               (hayt/where [[= :id (first obj)]])))

(defmethod parse :regions [table line]
  (let [[region-id region-name commnt] (map s/trim (s/split line #"\|"))]
    {:id (Long/parseLong region-id)
     :name region-name
     :comment commnt}))

(defmethod parse :nations [table line]
  (let [[nation-id nation-name region-id commnt] (map s/trim (s/split line #"\|"))]
    {:id (Long/parseLong nation-id)
     :name nation-name
     :region_id (Long/parseLong region-id)
     :comment commnt}))

(defmethod parse :customers [table line]
  (let [[cust-id cust-name address nation-id
         phone account-balance market-segment commnt] (map s/trim (s/split line #"\|"))]
    {:id (Long/parseLong cust-id)
     :name cust-name
     :address address
     :nation_id (Long/parseLong nation-id)
     :phone phone
     :account_balance (BigDecimal. ^String account-balance)
     :market_segment market-segment
     :comment commnt}))

(defmethod parse :orders [table line]
  (let [[order-id cust-id status total-price
         date priority clerk ship-priority commnt] (map s/trim (s/split line #"\|"))]
    {:id (Long/parseLong order-id)
     :customer_id (Long/parseLong cust-id)
     :status status
     :total_price (BigDecimal. ^String total-price)
     :date (c/to-date date)
     :priority priority
     :clerk clerk
     :ship_priority (Integer/parseInt ship-priority)
     :comment commnt}))

(defmethod parse :line_items [table line]
  (let [[order-id part-id supp-id
         line-number quantity extended-price discount
         tax return-flag status ship-date commit-date
         receipt-date ship-instruct ship-mode commnt] (map s/trim (s/split line #"\|"))]
    {:order_id (Long/parseLong order-id)
     :part_id (Long/parseLong part-id)
     :supplier_id (Long/parseLong supp-id)
     :line_number (Integer/parseInt line-number)
     :quantity (Integer/parseInt quantity)
     :extended_price (BigDecimal. ^String extended-price)
     :discount (BigDecimal. ^String discount)
     :tax (BigDecimal. ^String tax)
     :return_flag return-flag
     :status status
     :ship_date (c/to-date ship-date)
     :commit_date (c/to-date commit-date)
     :receipt_date (c/to-date receipt-date)
     :ship_instruct ship-instruct
     :ship_mode ship-mode
     :comment commnt}))

(defn load-data [file table]
  (with-open [reader (clojure.java.io/reader file)]
    (let [lines (line-seq reader)
          counter (atom 1)]
      (doseq [ls (partition-all 1000 lines)]
        (println (str "scheduling insert for " table " " (* @counter 1000)))
        (swap! counter inc)
        (future (perform-batch-insert table ls))))))

(defn load-all []
  (load-data "data/supplier.tbl" :suppliers)
  (load-data "data/part.tbl" :parts)
  (load-data "data/partsupp.tbl" :suppliers_parts)
  (load-data "data/region.tbl" :regions)
  (load-data "data/nation.tbl" :nations)
  (load-data "data/customer.tbl" :customers)
  (load-data "data/orders.tbl" :orders)
  (load-data "data/lineitem.tbl" :line_items))
