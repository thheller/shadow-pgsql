(defproject thheller/shadow-pgsql "0.13.0"
  :description "PostgreSQL Client for Java and Clojure"
  :url "http://github.com/thheller/shadow-pgsql"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :repositories
  {"clojars" {:url "https://clojars.org/repo"
              :sign-releases false}}

  :javac-options
  [ ;; "-target" "1.8"
   ;; "-source" "1.8"
   "--release" "8"]

  :dependencies
  [[org.apache.commons/commons-pool2 "2.2"]
   [io.dropwizard.metrics/metrics-core "3.1.2"]]

  :java-source-paths ["src/java"]
  :source-paths ["src/clj"]

  :profiles
  {:dev
   {:source-paths
    ["src/dev"
     "src/benchmark"]
    :dependencies
    [[org.clojure/clojure "1.8.0"]
     [org.clojure/tools.namespace "0.2.4"]
     [junit/junit "4.12"]]}

   :benchmark
   {:source-paths ["src/benchmark"]
    :java-source-paths ["src/benchmark"]
    :dependencies
    [[org.clojure/clojure "1.8.0"]
     [criterium "0.4.3"]]}})
