(ns flink-clj.connectors.file-test
  "Tests for flink-clj.connectors.file namespace."
  (:require [clojure.test :refer :all]
            [flink-clj.connectors.file :as file]
            [flink-clj.env :as env]))

;; Helper to detect Flink version
(defn- streaming-file-sink-available?
  "Check if StreamingFileSink class is available (Flink 1.x only)."
  []
  (try
    (Class/forName "org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink")
    true
    (catch ClassNotFoundException _ false)))

;; =============================================================================
;; StreamingFileSink Tests (Flink 1.x only)
;; =============================================================================

(deftest streaming-file-sink-test
  (if (streaming-file-sink-available?)
    (do
      (testing "Create basic streaming file sink"
        (let [sink (file/streaming-file-sink {:path "/tmp/test-output"})]
          (is (some? sink))))

      (testing "Create streaming file sink with rolling policy"
        (let [sink (file/streaming-file-sink
                     {:path "/tmp/test-output"
                      :rolling-policy {:part-size (* 1024 1024 128)
                                       :rollover-interval [15 :minutes]
                                       :inactivity-interval [5 :minutes]}})]
          (is (some? sink)))))
    (testing "StreamingFileSink not available in Flink 2.x"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"StreamingFileSink is not available"
                            (file/streaming-file-sink {:path "/tmp/test-output"}))))))

(deftest line-sink-test
  (if (streaming-file-sink-available?)
    (testing "Create line sink"
      (let [sink (file/line-sink {:path "/tmp/test-output"})]
        (is (some? sink))))
    (testing "Line sink not available in Flink 2.x"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"StreamingFileSink is not available"
                            (file/line-sink {:path "/tmp/test-output"}))))))

;; Helper to detect if readTextFile is available
(defn- read-text-file-available?
  []
  (try
    (let [env-class (Class/forName "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment")]
      (.getMethod env-class "readTextFile" (into-array Class [String String]))
      true)
    (catch NoSuchMethodException _ false)))

;; =============================================================================
;; Read Text File Tests
;; =============================================================================

(deftest read-text-file-test
  (if (read-text-file-available?)
    (testing "Create text file source"
      (let [env (env/create-env)
            ;; This creates a DataStream but we don't execute it
            stream (file/read-text-file env {:path "/tmp/test-input.txt"})]
        (is (some? stream))))
    (testing "readTextFile not available in Flink 2.x"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"readTextFile is not available"
                            (file/read-text-file (env/create-env) {:path "/tmp/test.txt"}))))))

;; Note: Full integration tests would require actual files and MiniCluster
;; Those would be marked as :integration tests
