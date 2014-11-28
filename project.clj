(defproject thheller/shadow-pgsql "0.7.0-SNAPSHOT"
  :description "PostgreSQL Client for Java and Clojure"
  :url "http://github.com/thheller/shadow-pgsql"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.apache.commons/commons-pool2 "2.2"]]

  :java-source-paths ["src/java"]
  :source-paths ["src/clj"]

  :profiles {:dev {:source-paths ["src/dev"]
                   :dependencies [[org.clojure/clojure "1.6.0"]
                                  [org.clojure/tools.namespace "0.2.4"]]}})
