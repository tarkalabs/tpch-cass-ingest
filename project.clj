(defproject tpch-cass-ingest "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-opts ["-server" "-XX:+UseConcMarkSweepGC"]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [cc.qbits/alia "2.0.0-rc3"]
                 [clj-time "0.7.0"]])
