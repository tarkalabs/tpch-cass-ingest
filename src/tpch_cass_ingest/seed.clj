(ns tpch-cass-ingest.seed
  (:require [clojure.string :as s]
            [qbits.alia :as alia]
            [tpch-cass-ingest.cassandra-connector :as cass]))

(def prepared-statements (atom {}))

(defn ps [line-type statement]
  (let [pstat (get @prepared-statements line-type)]
    (if pstat pstat
      (let [new-pstat (alia/prepare cass/session statement)]
        (swap! prepared-statements #(assoc % line-type new-pstat))
        new-pstat))))

(defmulti parse (fn [line-type line] line-type))
(defmulti inserter identity)
(defmulti perform-insert (fn [line-type obj] line-type))

(defmethod perform-insert :default [line-type obj]
  (alia/execute cass/session (inserter line-type) {:values obj}))

(defmethod parse :supplier [line-type line]
  (let [[supp-id supp-name address 
         nation-id phone account-balance commnt] (map s/trim (s/split line #"\|"))]
    [(Long/parseLong supp-id) supp-name address
     (Long/parseLong nation-id) phone 
     (BigDecimal. account-balance) commnt]))

(defmethod inserter :supplier [_]
  (ps :supplier "insert into suppliers(id,name,address,nation_id,phone,account_balance,comment) values(?,?,?,?,?,?,?)"))


(defmethod parse :part [line-type line]
  (let [[part-id part-name manufacturer
         brand part-type size container 
         retail-price cmmnt] (map s/trim (s/split line #"\|"))]
    [(Long/parseLong part-id) part-name manufacturer
     brand part-type (Integer/parseInt size) container 
     (BigDecimal. retail-price) cmmnt]))

(defmethod inserter :part [_]
  (ps :part "insert into parts(id,name,manufacturer,brand,type,size,container,retail_price,comment)
                             values(?,?,?,?,?,?,?,?,?)"))

(defmethod parse :supplier-part [line-type line]
  (let [[part-id supp-id quantity cost] (map s/trim (s/split line #"\|"))]
    [(Long/parseLong supp-id) (Long/parseLong part-id) (Integer/parseInt quantity) (BigDecimal. cost)]))

(defmethod inserter :supplier-part [_]
  (ps :supplier-part "insert into suppliers_parts (supplier_id, part_id, quantity,cost)
                             values(?,?,?,?)"))

(defmethod perform-insert :supplier-part [line-type obj]
  (let [inserter (inserter line-type)
        part-update (ps :update-part "update parts set suppliers=suppliers+? where id=?")
        supplier-update (ps :update-supplier "update suppliers set parts=parts+? where id=?")]
    (alia/execute cass/session inserter {:values obj})
    (alia/execute cass/session part-update {:values [ #{(first obj)} (second obj)]})
    (alia/execute cass/session supplier-update {:values [#{(second obj)} (first obj)]})))

(defn load-data [path line-type]
  (let [content (slurp path)
        lines (s/split-lines content)
        objects (map #(parse line-type %) lines)]
    (doseq [obj objects] (perform-insert line-type obj))))

(defn load-all []
  (load-data "data/partsupp.tbl" :supplier-part))

(defn -main []
  (load-all))

