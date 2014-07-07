(ns tpch-cass-ingest.cassandra-connector
  (:require [qbits.alia :as alia]))

(defonce session (alia/connect (alia/cluster) "tpch"))
