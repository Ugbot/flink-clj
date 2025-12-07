(ns flink-clj.env-test
  "Tests for environment creation and JAR/classpath loading."
  (:require [clojure.test :refer :all]
            [flink-clj.env :as env])
  (:import [org.apache.flink.streaming.api.environment StreamExecutionEnvironment]
           [org.apache.flink.configuration PipelineOptions]))

;; =============================================================================
;; Basic Environment Creation Tests
;; =============================================================================

(deftest create-env-test
  (testing "Create basic environment"
    (let [e (env/create-env)]
      (is (instance? StreamExecutionEnvironment e))))

  (testing "Create environment with parallelism"
    (let [e (env/create-env {:parallelism 4})]
      (is (instance? StreamExecutionEnvironment e))
      (is (= 4 (.getParallelism e)))))

  (testing "Create environment with max-parallelism"
    (let [e (env/create-env {:max-parallelism 128})]
      (is (instance? StreamExecutionEnvironment e))
      (is (= 128 (.getMaxParallelism e))))))

;; =============================================================================
;; JAR Loading Tests - create-env with :jars
;; =============================================================================

(deftest create-env-with-jars-test
  (testing "Create environment with single JAR"
    (let [jar-path "file:///opt/flink/connectors/kafka.jar"
          e (env/create-env {:jars [jar-path]})]
      (is (instance? StreamExecutionEnvironment e))
      (is (contains? (set (env/get-jars e)) jar-path))))

  (testing "Create environment with multiple JARs"
    (let [jar1 "file:///opt/flink/connectors/kafka.jar"
          jar2 "file:///opt/flink/connectors/avro.jar"
          e (env/create-env {:jars [jar1 jar2]})]
      (is (instance? StreamExecutionEnvironment e))
      (let [jars (set (env/get-jars e))]
        (is (contains? jars jar1))
        (is (contains? jars jar2)))))

  (testing "Create environment with JARs and parallelism"
    (let [e (env/create-env {:parallelism 4
                             :jars ["file:///test.jar"]})]
      (is (= 4 (.getParallelism e)))
      (is (contains? (set (env/get-jars e)) "file:///test.jar")))))

;; =============================================================================
;; Classpath Loading Tests - create-env with :classpaths
;; =============================================================================

(deftest create-env-with-classpaths-test
  (testing "Create environment with single classpath"
    (let [cp "file:///opt/flink/lib/"
          e (env/create-env {:classpaths [cp]})]
      (is (instance? StreamExecutionEnvironment e))
      (is (contains? (set (env/get-classpaths e)) cp))))

  (testing "Create environment with multiple classpaths"
    (let [cp1 "file:///opt/flink/lib/"
          cp2 "file:///opt/flink/plugins/"
          e (env/create-env {:classpaths [cp1 cp2]})]
      (is (instance? StreamExecutionEnvironment e))
      (let [cps (set (env/get-classpaths e))]
        (is (contains? cps cp1))
        (is (contains? cps cp2))))))

;; =============================================================================
;; Combined JARs and Classpaths Tests
;; =============================================================================

(deftest create-env-with-jars-and-classpaths-test
  (testing "Create environment with both JARs and classpaths"
    (let [jar "file:///connectors/kafka.jar"
          cp "file:///lib/"
          e (env/create-env {:jars [jar]
                             :classpaths [cp]})]
      (is (instance? StreamExecutionEnvironment e))
      (is (contains? (set (env/get-jars e)) jar))
      (is (contains? (set (env/get-classpaths e)) cp)))))

;; =============================================================================
;; add-jars! Tests
;; =============================================================================

(deftest add-jars-test
  (testing "Add single JAR to environment"
    (let [e (env/create-env)
          jar "file:///test/connector.jar"
          result (env/add-jars! e jar)]
      (is (= e result) "add-jars! should return the environment")
      (is (contains? (set (env/get-jars e)) jar))))

  (testing "Add multiple JARs to environment"
    (let [e (env/create-env)
          jar1 "file:///test/kafka.jar"
          jar2 "file:///test/avro.jar"
          jar3 "file:///test/json.jar"]
      (env/add-jars! e jar1 jar2 jar3)
      (let [jars (set (env/get-jars e))]
        (is (contains? jars jar1))
        (is (contains? jars jar2))
        (is (contains? jars jar3)))))

  (testing "Add JARs incrementally"
    (let [e (env/create-env)
          jar1 "file:///first.jar"
          jar2 "file:///second.jar"]
      (env/add-jars! e jar1)
      (env/add-jars! e jar2)
      (let [jars (set (env/get-jars e))]
        (is (contains? jars jar1))
        (is (contains? jars jar2)))))

  (testing "Add JARs via threading"
    (let [e (-> (env/create-env)
                (env/add-jars! "file:///a.jar")
                (env/add-jars! "file:///b.jar"))]
      (let [jars (set (env/get-jars e))]
        (is (contains? jars "file:///a.jar"))
        (is (contains? jars "file:///b.jar"))))))

;; =============================================================================
;; add-classpaths! Tests
;; =============================================================================

(deftest add-classpaths-test
  (testing "Add single classpath to environment"
    (let [e (env/create-env)
          cp "file:///lib/"
          result (env/add-classpaths! e cp)]
      (is (= e result) "add-classpaths! should return the environment")
      (is (contains? (set (env/get-classpaths e)) cp))))

  (testing "Add multiple classpaths to environment"
    (let [e (env/create-env)
          cp1 "file:///lib1/"
          cp2 "file:///lib2/"]
      (env/add-classpaths! e cp1 cp2)
      (let [cps (set (env/get-classpaths e))]
        (is (contains? cps cp1))
        (is (contains? cps cp2))))))

;; =============================================================================
;; Combined add-jars! and add-classpaths! Tests
;; =============================================================================

(deftest add-jars-and-classpaths-combined-test
  (testing "PyFlink-style chained configuration"
    (let [e (-> (env/create-env)
                (env/add-jars! "file:///kafka.jar" "file:///avro.jar")
                (env/add-classpaths! "file:///lib/"))]
      (let [jars (set (env/get-jars e))
            cps (set (env/get-classpaths e))]
        (is (contains? jars "file:///kafka.jar"))
        (is (contains? jars "file:///avro.jar"))
        (is (contains? cps "file:///lib/"))))))

;; =============================================================================
;; jar-url Helper Tests
;; =============================================================================

(deftest jar-url-test
  (testing "Convert absolute path to file URL"
    (is (= "file:///opt/flink/kafka.jar"
           (env/jar-url "/opt/flink/kafka.jar"))))

  (testing "Convert relative path to file URL"
    (is (= "file:///connectors/kafka.jar"
           (env/jar-url "connectors/kafka.jar"))))

  (testing "Pass through existing file:// URL"
    (is (= "file:///already/formatted.jar"
           (env/jar-url "file:///already/formatted.jar"))))

  (testing "Pass through http:// URL"
    (is (= "http://example.com/connector.jar"
           (env/jar-url "http://example.com/connector.jar"))))

  (testing "Pass through https:// URL"
    (is (= "https://repo.maven.org/kafka.jar"
           (env/jar-url "https://repo.maven.org/kafka.jar")))))

;; =============================================================================
;; maven-jar Helper Tests
;; =============================================================================

(deftest maven-jar-test
  (testing "Generate Maven Central URL for Flink Kafka connector"
    (is (= "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.20.0/flink-connector-kafka-1.20.0.jar"
           (env/maven-jar "org.apache.flink" "flink-connector-kafka" "1.20.0"))))

  (testing "Generate Maven Central URL for nested group ID"
    (is (= "https://repo1.maven.org/maven2/com/example/deep/nested/my-lib/2.0.0/my-lib-2.0.0.jar"
           (env/maven-jar "com.example.deep.nested" "my-lib" "2.0.0"))))

  (testing "Generate Maven Central URL for simple group ID"
    (is (= "https://repo1.maven.org/maven2/org/clojure/1.11.1/clojure-1.11.1.jar"
           (env/maven-jar "org" "clojure" "1.11.1")))))

;; =============================================================================
;; Integration-style Usage Tests
;; =============================================================================

(deftest realistic-usage-test
  (testing "Kafka connector setup pattern"
    (let [kafka-jar (env/maven-jar "org.apache.flink" "flink-connector-kafka" "1.20.0")
          e (env/create-env {:parallelism 4
                             :jars [kafka-jar]})]
      (is (= 4 (.getParallelism e)))
      (is (contains? (set (env/get-jars e)) kafka-jar))))

  (testing "Local JAR with jar-url helper"
    (let [local-jar (env/jar-url "/home/user/connectors/custom.jar")
          e (env/create-env {:jars [local-jar]})]
      (is (= "file:///home/user/connectors/custom.jar" local-jar))
      (is (contains? (set (env/get-jars e)) local-jar))))

  (testing "Mixed Maven and local JARs"
    (let [maven-jar (env/maven-jar "org.apache.flink" "flink-sql-avro" "1.20.0")
          local-jar (env/jar-url "/opt/custom-udf.jar")
          e (env/create-env {:jars [maven-jar local-jar]})]
      (let [jars (set (env/get-jars e))]
        (is (contains? jars maven-jar))
        (is (contains? jars local-jar))))))

;; =============================================================================
;; Empty/Edge Case Tests
;; =============================================================================

(deftest edge-cases-test
  (testing "Empty jars vector"
    (let [e (env/create-env {:jars []})]
      (is (instance? StreamExecutionEnvironment e))))

  (testing "Empty classpaths vector"
    (let [e (env/create-env {:classpaths []})]
      (is (instance? StreamExecutionEnvironment e))))

  (testing "Nil jars (should not throw)"
    (let [e (env/create-env {:jars nil})]
      (is (instance? StreamExecutionEnvironment e))))

  (testing "Get jars when none configured"
    (let [e (env/create-env)]
      (is (vector? (env/get-jars e)))
      (is (empty? (env/get-jars e)))))

  (testing "Get classpaths when none configured"
    (let [e (env/create-env)]
      (is (vector? (env/get-classpaths e)))
      (is (empty? (env/get-classpaths e))))))
