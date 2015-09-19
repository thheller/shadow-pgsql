(defproject thheller/shadow-pgsql "0.8.0-SNAPSHOT"
  :description "PostgreSQL Client for Java and Clojure"
  :url "http://github.com/thheller/shadow-pgsql"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.apache.commons/commons-pool2 "2.2"]
                 [io.dropwizard.metrics/metrics-core "3.1.2"]]

  :java-source-paths ["src/java"]
  :source-paths ["src/clj"]

  :profiles {:dev {:source-paths ["src/dev"
                                  "src/benchmark"]
                   :dependencies [[org.clojure/clojure "1.7.0"]
                                  [org.clojure/tools.namespace "0.2.4"]]}

             :benchmark {:source-paths ["src/benchmark"]
                         :java-source-paths ["src/benchmark"]
                         :dependencies [[org.clojure/clojure "1.7.0"]
                                        [criterium "0.4.3"]
                                        [org.postgresql/postgresql "9.4-1203-jdbc42"]]}})
