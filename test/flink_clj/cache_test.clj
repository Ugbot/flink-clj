(ns flink-clj.cache-test
  "Tests for distributed cache functionality."
  (:require [clojure.test :refer :all]
            [flink-clj.cache :as cache])
  (:import [java.io File]
           [java.nio.file Files]))

;; =============================================================================
;; API Structure Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "Registration functions exist"
    (is (fn? cache/register-file))
    (is (fn? cache/register-files)))

  (testing "Access functions exist"
    (is (fn? cache/get-file))
    (is (fn? cache/get-file-path)))

  (testing "File reading utilities exist"
    (is (fn? cache/read-lines))
    (is (fn? cache/read-string))
    (is (fn? cache/read-edn))
    (is (fn? cache/read-json))
    (is (fn? cache/read-csv))
    (is (fn? cache/lines-seq))
    (is (fn? cache/build-lookup)))

  (testing "Info function exists"
    (is (fn? cache/cache-info))))

;; =============================================================================
;; File Reading Tests (with temp files)
;; =============================================================================

(defn create-temp-file
  "Create a temporary file with given content."
  [content suffix]
  (let [file (File/createTempFile "cache-test" suffix)]
    (.deleteOnExit file)
    (spit file content)
    file))

(deftest read-lines-test
  (testing "read-lines returns vector of strings"
    (let [file (create-temp-file "line1\nline2\nline3" ".txt")
          lines (cache/read-lines file)]
      (is (vector? lines))
      (is (= 3 (count lines)))
      (is (= "line1" (first lines)))
      (is (= "line3" (last lines)))))

  (testing "read-lines handles empty file"
    (let [file (create-temp-file "" ".txt")
          lines (cache/read-lines file)]
      (is (empty? lines)))))

(deftest read-string-test
  (testing "read-string returns full content"
    (let [content "Hello\nWorld\n!"
          file (create-temp-file content ".txt")
          result (cache/read-string file)]
      (is (= content result))))

  (testing "read-string handles unicode"
    (let [content "Hello \u4e16\u754c!"
          file (create-temp-file content ".txt")
          result (cache/read-string file)]
      (is (= content result)))))

(deftest read-edn-test
  (testing "read-edn parses EDN data"
    (let [data {:name "test" :values [1 2 3] :nested {:a 1}}
          file (create-temp-file (pr-str data) ".edn")
          result (cache/read-edn file)]
      (is (= data result))))

  (testing "read-edn handles vectors"
    (let [data [1 2 3 4 5]
          file (create-temp-file (pr-str data) ".edn")
          result (cache/read-edn file)]
      (is (= data result)))))

(deftest read-csv-test
  (testing "read-csv parses basic CSV"
    (let [file (create-temp-file "a,b,c\n1,2,3\n4,5,6" ".csv")
          result (cache/read-csv file)]
      (is (= 3 (count result)))
      (is (= ["a" "b" "c"] (first result)))
      (is (= ["1" "2" "3"] (second result)))))

  (testing "read-csv with header"
    (let [file (create-temp-file "name,age,city\nAlice,30,NYC\nBob,25,LA" ".csv")
          result (cache/read-csv file {:header? true})]
      (is (= 2 (count result)))
      (is (= "Alice" (:name (first result))))
      (is (= "30" (:age (first result)))))

  (testing "read-csv with custom separator"
    (let [file (create-temp-file "a;b;c\n1;2;3" ".csv")
          result (cache/read-csv file {:separator \;})]
      (is (= ["a" "b" "c"] (first result)))))))

(deftest read-json-test
  (testing "read-json requires json library"
    (let [file (create-temp-file "{\"key\": \"value\"}" ".json")]
      ;; This will either work (if cheshire available) or throw
      (try
        (let [result (cache/read-json file)]
          (is (map? result)))
        (catch clojure.lang.ExceptionInfo e
          (is (re-find #"JSON parsing requires" (.getMessage e))))))))

(deftest lines-seq-test
  (testing "lines-seq returns lazy sequence with reader"
    (let [file (create-temp-file "line1\nline2\nline3" ".txt")
          [reader line-s] (cache/lines-seq file)]
      (try
        (is (some? reader))
        (is (seq? line-s))
        (is (= "line1" (first line-s)))
        (finally
          (.close reader))))))

;; =============================================================================
;; Lookup Table Builder Tests
;; =============================================================================

(deftest build-lookup-test
  (testing "build-lookup from lines"
    (let [file (create-temp-file "key1=val1\nkey2=val2\nkey3=val3" ".txt")
          lookup (cache/build-lookup file
                   (fn [line] (first (.split line "=")))
                   (fn [line] (second (.split line "="))))]
      (is (= "val1" (get lookup "key1")))
      (is (= "val2" (get lookup "key2")))
      (is (= "val3" (get lookup "key3")))))

  (testing "build-lookup from CSV"
    ;; With header? true, rows become maps with keyword keys
    (let [file (create-temp-file "id,name\n1,Alice\n2,Bob" ".csv")
          lookup (cache/build-lookup file
                   :id    ; key is the :id field
                   :name  ; value is the :name field
                   {:format :csv :header? true})]
      (is (= "Alice" (get lookup "1")))
      (is (= "Bob" (get lookup "2")))))

  (testing "build-lookup from EDN"
    (let [data [{:id "a" :value 1} {:id "b" :value 2}]
          file (create-temp-file (pr-str data) ".edn")
          lookup (cache/build-lookup file
                   :id
                   :value
                   {:format :edn})]
      (is (= 1 (get lookup "a")))
      (is (= 2 (get lookup "b"))))))

;; =============================================================================
;; Cache Info Test
;; =============================================================================

(deftest cache-info-test
  (testing "cache-info returns expected structure"
    (let [info (cache/cache-info)]
      (is (map? info))
      (is (contains? info :description))
      (is (contains? info :features))
      (is (contains? info :supported-filesystems))
      (is (contains? info :usage-notes))
      (is (sequential? (:features info)))
      (is (sequential? (:supported-filesystems info))))))

;; =============================================================================
;; Registration Tests (require env - placeholders)
;; =============================================================================

(deftest ^:integration register-file-test
  (testing "register-file adds file to env"
    ;; Would need StreamExecutionEnvironment
    (is true "Integration test placeholder")))

(deftest ^:integration register-files-test
  (testing "register-files adds multiple files"
    (is true "Integration test placeholder")))

(deftest ^:integration get-file-test
  (testing "get-file retrieves cached file from RuntimeContext"
    (is true "Integration test placeholder")))
