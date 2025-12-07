(defproject io.github.ugbot/flink-clj "0.1.0-SNAPSHOT"
  :description "A Clojure toolkit for Apache Flink"
  :url "https://github.com/Ugbot/flink-clj"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.taoensso/nippy "3.3.0"]
                 [org.clojure/tools.cli "1.0.219"]
                 [org.clojure/data.json "2.4.0"]]

  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :resource-paths ["resources"]
  :test-paths ["test"]

  :plugins [[lein-exec "0.3.7"]]

  :profiles
  {:dev {:dependencies [[org.clojure/test.check "1.1.1"]
                        [org.clojure/tools.namespace "1.4.4"]
                        [criterium "0.4.6"]
                        ;; Kafka client for examples (producing test messages)
                        [org.apache.kafka/kafka-clients "3.6.1"]
                        ;; PostgreSQL driver for examples
                        [org.postgresql/postgresql "42.7.1"]
                        ;; Serialization libraries for testing
                        [cheshire "5.12.0"]
                        [org.clojure/data.json "2.4.0"]
                        [com.cognitect/transit-clj "1.0.333"]
                        ;; Avro (optional, for avro tests)
                        [org.apache.avro/avro "1.11.3"]]
         :source-paths ["dev" "examples" "examples/jobs"]
         :repl-options {:init-ns user}}

   :flink-1.20 {:dependencies
                [[org.apache.flink/flink-streaming-java "1.20.0" :scope "provided"]
                 [org.apache.flink/flink-clients "1.20.0" :scope "provided"]
                 [org.apache.flink/flink-test-utils "1.20.0" :scope "test"]
                 [org.apache.flink/flink-connector-kafka "3.3.0-1.20" :scope "provided"]
                 [org.apache.flink/flink-connector-base "1.20.0" :scope "provided"]
                 [org.apache.flink/flink-cep "1.20.0" :scope "provided"]]
                :java-source-paths ["src/java" "src/java-1.20"]
                :classifiers {:flink-1.20 :default}}

   :flink-2.x {:dependencies
               [[org.apache.flink/flink-streaming-java "2.1.0" :scope "provided"]
                [org.apache.flink/flink-clients "2.1.0" :scope "provided"]
                [org.apache.flink/flink-test-utils "2.1.0" :scope "test"]
                [org.apache.flink/flink-cep "2.1.0" :scope "provided"]
                [org.apache.flink/flink-connector-base "2.1.0" :scope "provided"]]
               :java-source-paths ["src/java" "src/java-2.x"]
               :classifiers {:flink-2.x :default}}

   :uberjar {:aot :all
             :omit-source true}}

  :aot [flink-clj.impl.functions
        flink-clj.impl.kryo
        flink-clj.cli.core]

  :main flink-clj.cli.core

  :javac-options ["-target" "11" "-source" "11" "-Xlint:unchecked"]

  :deploy-repositories [["clojars" {:url "https://clojars.org/repo"
                                    :sign-releases false}]]

  :aliases {"test-1.20" ["with-profile" "+flink-1.20,+dev" "test"]
            "test-2.x" ["with-profile" "+flink-2.x,+dev" "test"]
            "test-all" ["do" ["test-1.20"] ["test-2.x"]]
            "compile-1.20" ["with-profile" "+flink-1.20" "compile"]
            "compile-2.x" ["with-profile" "+flink-2.x" "compile"]
            "jar-1.20" ["with-profile" "+flink-1.20" "jar"]
            "jar-2.x" ["with-profile" "+flink-2.x" "jar"]
            "shell" ["with-profile" "+flink-1.20,+dev" "repl"]
            "shell-2.x" ["with-profile" "+flink-2.x,+dev" "repl"]
            "word-count" ["with-profile" "+flink-1.20,+dev"
                          "exec" "-ep" "(require 'word-count) (word-count/-main)"]
            "run-example" ["with-profile" "+flink-1.20,+dev" "exec" "-ep"]
            ;; CLI commands
            "cli" ["with-profile" "+flink-1.20,+dev" "run" "-m" "flink-clj.cli.core"]
            "cli-jar" ["with-profile" "+flink-1.20,+uberjar" "uberjar"]}

  :repositories [["central" "https://repo1.maven.org/maven2/"]
                 ["clojars" "https://clojars.org/repo"]]

  :min-lein-version "2.9.0")
