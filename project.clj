(defproject shadow-pgsql "0.1.0-SNAPSHOT"
  :description "PostgreSQL Client for Java and Clojure"
  :url "http://github.com/thheller/shadow-pgsql"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  
  :java-source-paths ["src/java"]
  :source-paths ["src/clj"]

  :profiles {:dev {:dependencies [[org.clojure/clojure "1.6.0"]]}})
