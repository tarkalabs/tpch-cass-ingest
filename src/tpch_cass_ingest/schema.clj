(ns tpch-cass-ingest.schema
  (:require [qbits.alia :as alia]
            [tpch-cass-ingest.cassandra-connector :as cass]))

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
                            
(defn setup-schema []
  (setup-suppliers)
  (setup-parts)
  (setup-part-suppliers))
