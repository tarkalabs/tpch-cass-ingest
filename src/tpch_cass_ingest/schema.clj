(ns tpch-cass-ingest.schema
  (:require [qbits.alia :as alia]
            [tpch-cass-ingest.cassandra-connector :as cass]))

(defn setup-regions []
  (alia/execute cass/session "create table regions (id bigint primary key,
                                           name varchar,
                                           comment varchar)"))

(defn setup-nations []
  (alia/execute cass/session "create table nations (id bigint primary key,
                                           name varchar,
                                           region_id bigint,
                                           comment varchar)"))

(defn setup-suppliers []
  (alia/execute cass/session "create table suppliers (id bigint primary key,
                                           name varchar,
                                           address varchar,
                                           nation_id bigint,
                                           phone varchar,
                                           account_balance decimal,
                                           parts set<bigint>,
                                           comment varchar)"))
(defn setup-parts []
  (alia/execute cass/session "create table parts (id bigint primary key,
                                           name varchar,
                                           manufacturer varchar,
                                           brand varchar,
                                           type varchar,
                                           size int,
                                           container varchar,
                                           retail_price decimal,
                                           suppliers set<bigint>,
                                           comment varchar)"))
(defn setup-part-suppliers []
  (alia/execute cass/session "create table suppliers_parts (supplier_id bigint,
                                           part_id bigint,
                                           quantity int,
                                           cost decimal,
                                           primary key (supplier_id, part_id))"))

(defn setup-customers []
  (alia/execute cass/session "create table customers (id bigint primary key,
                                           name varchar,
                                           address varchar,
                                           nation_id bigint,
                                           phone varchar,
                                           account_balance decimal,
                                           market_segment varchar,
                                           comment varchar)"))

(defn setup-orders []
  (alia/execute cass/session "create table orders (id bigint primary key,
                                           customer_id bigint,
                                           status varchar,
                                           total_price decimal,
                                           date timestamp,
                                           priority varchar,
                                           clerk varchar,
                                           ship_priority int,
                                           comment varchar)"))

(defn setup-lineitems []
  (alia/execute cass/session "create table line_items (order_id bigint,
                             part_id bigint,
                             supplier_id bigint,
                             line_number int,
                             quantity int,
                             extended_price decimal,
                             discount decimal,
                             tax decimal,
                             return_flag varchar,
                             status varchar,
                             ship_date timestamp,
                             commit_date timestamp,
                             receipt_date timestamp,
                             ship_instruct varchar,
                             ship_mode varchar,
                             comment varchar,
                             primary key (order_id, line_number))"))

(defn setup-schema []
  (setup-regions)
  (setup-nations)
  (setup-suppliers)
  (setup-parts)
  (setup-part-suppliers)
  (setup-customers)
  (setup-orders)
  (setup-lineitems))
