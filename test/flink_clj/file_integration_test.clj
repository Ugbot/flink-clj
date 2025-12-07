(ns flink-clj.file-integration-test
  "Integration tests for file connector construction.

  Note: Full file I/O testing requires a production-like environment.
  These tests verify that file sources and sinks can be constructed
  correctly with various configurations."
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [flink-clj.env :as env]
            [flink-clj.connectors.file :as file])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

;; =============================================================================
;; Test Helpers
;; =============================================================================

(defn- streaming-file-sink-available?
  "Check if StreamingFileSink class is available (Flink 1.x only)."
  []
  (try
    (Class/forName "org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink")
    true
    (catch ClassNotFoundException _ false)))

(defn- read-text-file-available?
  "Check if readTextFile method is available (deprecated in Flink 2.x)."
  []
  (try
    (let [env-class (Class/forName "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment")]
      (.getMethod env-class "readTextFile" (into-array Class [String String]))
      true)
    (catch NoSuchMethodException _ false)))

(defn create-temp-dir
  "Create a temporary directory for test files."
  []
  (let [dir (Files/createTempDirectory "flink-test-" (into-array FileAttribute []))]
    (.toFile dir)))

(defn delete-recursively
  "Delete a directory and all its contents."
  [^File f]
  (when (.isDirectory f)
    (doseq [child (.listFiles f)]
      (delete-recursively child)))
  (.delete f))

(defn write-test-file
  "Write lines to a test file."
  [^File file lines]
  (spit file (clojure.string/join "\n" lines)))

;; =============================================================================
;; StreamingFileSink Construction Tests (Flink 1.x only)
;; =============================================================================

(deftest streaming-file-sink-basic-test
  (if (streaming-file-sink-available?)
    (testing "Create basic StreamingFileSink"
      (let [temp-dir (create-temp-dir)]
        (try
          (let [sink (file/streaming-file-sink {:path (.getAbsolutePath temp-dir)})]
            (is (some? sink)))
          (finally
            (delete-recursively temp-dir)))))
    (testing "StreamingFileSink not available in Flink 2.x"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"StreamingFileSink is not available"
                            (file/streaming-file-sink {:path "/tmp/test"}))))))

(deftest streaming-file-sink-with-rolling-policy-test
  (if (streaming-file-sink-available?)
    (testing "Create StreamingFileSink with rolling policy"
      (let [temp-dir (create-temp-dir)]
        (try
          (let [sink (file/streaming-file-sink
                       {:path (.getAbsolutePath temp-dir)
                        :rolling-policy {:part-size (* 1024 1024 128)
                                         :rollover-interval [15 :minutes]
                                         :inactivity-interval [5 :minutes]}})]
            (is (some? sink)))
          (finally
            (delete-recursively temp-dir)))))
    (testing "StreamingFileSink not available in Flink 2.x"
      (is true))))

(deftest line-sink-test
  (if (streaming-file-sink-available?)
    (testing "Create line sink"
      (let [temp-dir (create-temp-dir)]
        (try
          (let [sink (file/line-sink {:path (.getAbsolutePath temp-dir)})]
            (is (some? sink)))
          (finally
            (delete-recursively temp-dir)))))
    (testing "Line sink not available in Flink 2.x"
      (is true))))

;; =============================================================================
;; File Source Construction Tests
;; =============================================================================

(deftest read-text-file-construction-test
  (if (read-text-file-available?)
    (testing "read-text-file creates DataStream"
      (let [temp-dir (create-temp-dir)
            test-file (File. temp-dir "input.txt")]
        (try
          (write-test-file test-file ["line1" "line2" "line3"])
          (let [flink-env (env/create-env)
                stream (file/read-text-file flink-env {:path (.getAbsolutePath test-file)})]
            (is (some? stream)))
          (finally
            (delete-recursively temp-dir)))))
    (testing "readTextFile not available in Flink 2.x"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"readTextFile is not available"
                            (file/read-text-file (env/create-env) {:path "/tmp/test.txt"}))))))

(deftest read-text-file-with-charset-test
  (if (read-text-file-available?)
    (testing "read-text-file accepts charset option"
      (let [temp-dir (create-temp-dir)
            test-file (File. temp-dir "input.txt")]
        (try
          (write-test-file test-file ["hello" "world"])
          (let [flink-env (env/create-env)
                stream (file/read-text-file flink-env
                                            {:path (.getAbsolutePath test-file)
                                             :charset "UTF-8"})]
            (is (some? stream)))
          (finally
            (delete-recursively temp-dir)))))
    (testing "readTextFile not available in Flink 2.x"
      (is true))))

(deftest read-text-file-parallel-construction-test
  (if (read-text-file-available?)
    (testing "read-text-file-parallel creates DataStream"
      (let [temp-dir (create-temp-dir)
            file1 (File. temp-dir "part1.txt")
            file2 (File. temp-dir "part2.txt")]
        (try
          (write-test-file file1 ["a" "b"])
          (write-test-file file2 ["c" "d"])
          (let [flink-env (env/create-env)
                stream (file/read-text-file-parallel
                         flink-env
                         {:paths [(.getAbsolutePath file1)
                                  (.getAbsolutePath file2)]})]
            (is (some? stream)))
          (finally
            (delete-recursively temp-dir)))))
    (testing "readTextFile not available in Flink 2.x"
      (is true))))

;; =============================================================================
;; Rolling Policy Duration Conversion Tests (Flink 1.x only)
;; =============================================================================

(deftest rolling-policy-durations-test
  (if (streaming-file-sink-available?)
    (testing "Various duration formats in rolling policy"
      (let [temp-dir (create-temp-dir)]
        (try
          ;; Test with milliseconds
          (let [sink1 (file/streaming-file-sink
                        {:path (.getAbsolutePath temp-dir)
                         :rolling-policy {:rollover-interval [1000 :ms]}})]
            (is (some? sink1)))
          ;; Test with seconds
          (let [sink2 (file/streaming-file-sink
                        {:path (.getAbsolutePath temp-dir)
                         :rolling-policy {:rollover-interval [60 :seconds]}})]
            (is (some? sink2)))
          ;; Test with hours
          (let [sink3 (file/streaming-file-sink
                        {:path (.getAbsolutePath temp-dir)
                         :rolling-policy {:rollover-interval [1 :hours]}})]
            (is (some? sink3)))
          (finally
            (delete-recursively temp-dir)))))
    (testing "Rolling policy not applicable in Flink 2.x"
      (is true))))
