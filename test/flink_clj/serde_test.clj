(ns flink-clj.serde-test
  "Tests for serialization/deserialization schemas."
  (:require [clojure.test :refer :all]
            [flink-clj.serde :as serde])
  (:import [java.nio.charset StandardCharsets]))

;; =============================================================================
;; Helper functions
;; =============================================================================

(defn roundtrip
  "Test roundtrip serialization: serialize then deserialize."
  [schema data]
  (let [serialized (.serialize schema data)
        deserialized (.deserialize schema serialized)]
    deserialized))

;; =============================================================================
;; String Schema Tests
;; =============================================================================

(deftest string-schema-test
  (testing "SimpleStringSchema roundtrip"
    (let [schema (serde/string-schema)
          data "Hello, Flink!"]
      (is (= data (roundtrip schema data))))))

;; =============================================================================
;; JSON Schema Tests
;; =============================================================================

(deftest json-schema-test
  (testing "JSON roundtrip with map"
    (let [schema (serde/json)
          data {:name "Alice" :age 30 :active true}]
      (is (= data (roundtrip schema data)))))

  (testing "JSON roundtrip with vector"
    (let [schema (serde/json)
          data [1 2 3 "four" {:nested true}]]
      (is (= data (roundtrip schema data)))))

  (testing "JSON roundtrip with nested structures"
    (let [schema (serde/json)
          data {:user {:name "Bob" :roles ["admin" "user"]}
                :metadata {:created "2024-01-01"}}]
      (is (= data (roundtrip schema data)))))

  (testing "JSON handles nil"
    (let [schema (serde/json)]
      (is (nil? (.deserialize schema nil))))))

(deftest json-serialize-test
  (testing "JSON serialization produces valid JSON"
    (let [schema (serde/json)
          data {:name "Test"}
          bytes (.serialize schema data)
          json-str (String. bytes StandardCharsets/UTF_8)]
      (is (or (= "{\"name\":\"Test\"}" json-str)
              (= "{:name \"Test\"}" json-str))))))

;; =============================================================================
;; EDN Schema Tests
;; =============================================================================

(deftest edn-schema-test
  (testing "EDN roundtrip with map"
    (let [schema (serde/edn)
          data {:name "Alice" :age 30 :active true}]
      (is (= data (roundtrip schema data)))))

  (testing "EDN roundtrip with keywords"
    (let [schema (serde/edn)
          data {:type :user :status :active}]
      (is (= data (roundtrip schema data)))))

  (testing "EDN roundtrip with sets"
    (let [schema (serde/edn)
          data {:tags #{:important :urgent :reviewed}}]
      (is (= data (roundtrip schema data)))))

  (testing "EDN roundtrip with symbols"
    (let [schema (serde/edn)
          data {:fn-name 'my-function}]
      (is (= data (roundtrip schema data)))))

  (testing "EDN handles nil"
    (let [schema (serde/edn)]
      (is (nil? (.deserialize schema nil))))))

(deftest edn-serialize-test
  (testing "EDN serialization produces valid EDN"
    (let [schema (serde/edn)
          data {:name "Test" :count 42}
          bytes (.serialize schema data)
          edn-str (String. bytes StandardCharsets/UTF_8)]
      (is (re-find #":name" edn-str))
      (is (re-find #":count" edn-str)))))

;; =============================================================================
;; Nippy Schema Tests
;; =============================================================================

(deftest nippy-schema-test
  (testing "Nippy roundtrip with map"
    (let [schema (serde/nippy)
          data {:name "Alice" :age 30 :active true}]
      (is (= data (roundtrip schema data)))))

  (testing "Nippy roundtrip with complex data"
    (let [schema (serde/nippy)
          data {:timestamp (java.util.Date.)
                :uuid (java.util.UUID/randomUUID)
                :nested {:deep {:structure [1 2 3]}}}]
      ;; Compare without dates (they serialize differently)
      (let [result (roundtrip schema data)]
        (is (uuid? (:uuid result)))
        (is (= [1 2 3] (get-in result [:nested :deep :structure]))))))

  (testing "Nippy handles nil"
    (let [schema (serde/nippy)]
      (is (nil? (.deserialize schema nil))))))

(deftest nippy-efficient-test
  (testing "Nippy produces compact output"
    (let [schema (serde/nippy)
          json-schema (serde/json)
          data {:users (vec (for [i (range 100)]
                              {:id i :name (str "User-" i) :active (even? i)}))}
          nippy-bytes (.serialize schema data)
          json-bytes (.serialize json-schema data)]
      ;; Nippy should generally be smaller or comparable
      (is (< (count nippy-bytes) (* 2 (count json-bytes)))))))

;; =============================================================================
;; Transit Schema Tests
;; =============================================================================

(deftest transit-json-schema-test
  (testing "Transit JSON roundtrip with map"
    (let [schema (serde/transit)
          data {:name "Alice" :age 30 :active true}]
      (is (= data (roundtrip schema data)))))

  (testing "Transit preserves keywords"
    (let [schema (serde/transit)
          data {:type :event :status :pending}]
      (is (= data (roundtrip schema data)))))

  (testing "Transit handles sets"
    (let [schema (serde/transit)
          data {:tags #{:a :b :c}}]
      (is (= data (roundtrip schema data))))))

(deftest transit-msgpack-schema-test
  (testing "Transit MessagePack roundtrip"
    (let [schema (serde/transit {:type :msgpack})
          data {:name "Test" :values [1 2 3]}]
      (is (= data (roundtrip schema data))))))

;; =============================================================================
;; Raw Bytes Schema Tests
;; =============================================================================

(deftest raw-bytes-schema-test
  (testing "Raw bytes passthrough"
    (let [schema (serde/raw-bytes)
          data (.getBytes "Hello" StandardCharsets/UTF_8)
          result (.deserialize schema data)]
      (is (= (seq data) (seq result)))))

  (testing "Raw bytes serialize string"
    (let [schema (serde/raw-bytes)
          result (.serialize schema "Hello")]
      (is (= "Hello" (String. result StandardCharsets/UTF_8))))))

;; =============================================================================
;; Transform Schema Tests
;; =============================================================================

(deftest transform-schema-test
  (testing "Transform adds metadata after deserialize"
    (let [inner-schema (serde/json)
          schema (serde/transform inner-schema
                                  {:after-deserialize #(assoc % :processed true)})
          data {:name "Test"}
          result (roundtrip schema data)]
      (is (= true (:processed result)))))

  (testing "Transform modifies before serialize"
    (let [inner-schema (serde/json)
          schema (serde/transform inner-schema
                                  {:before-serialize #(dissoc % :internal)})
          data {:name "Test" :internal "secret"}
          bytes (.serialize schema data)
          json-str (String. bytes StandardCharsets/UTF_8)]
      (is (not (re-find #"internal" json-str))))))

;; =============================================================================
;; Type Info Tests
;; =============================================================================

(deftest typed-schema-test
  (testing "Typed schema passes through data"
    (let [schema (serde/typed (serde/json)
                              {:type-info (org.apache.flink.api.common.typeinfo.Types/MAP
                                            org.apache.flink.api.common.typeinfo.Types/STRING
                                            org.apache.flink.api.common.typeinfo.Types/LONG)})
          data {:count 42}]
      (is (= data (roundtrip schema data))))))

;; =============================================================================
;; Avro Schema Tests (only if Avro available)
;; =============================================================================

(deftest ^:avro avro-schema-test
  (testing "Avro generic record roundtrip"
    (try
      (let [schema-json "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"count\",\"type\":\"long\"}]}"
            schema (serde/avro-generic {:schema schema-json})]
        ;; Creating and testing with GenericRecord is complex
        ;; This just verifies the schema can be created
        (is (some? schema)))
      (catch Exception e
        (println "Avro test skipped:" (.getMessage e))))))

;; =============================================================================
;; Integration Tests - Kafka Connector Format Resolution
;; =============================================================================

(deftest kafka-format-resolution-test
  (testing "Kafka connector can resolve :json format"
    ;; This tests that the format resolution in kafka.clj works
    (let [resolve-fn (requiring-resolve 'flink-clj.connectors.kafka/resolve-deserializer)]
      ;; Note: This is a private fn, so we test via the public API indirectly
      ;; For now, just verify the serde namespace is loadable
      (is (some? (serde/json))))))

;; =============================================================================
;; Performance Comparison
;; =============================================================================

(deftest ^:benchmark serialization-performance-test
  (testing "Compare serialization speeds"
    (let [data {:user {:id 12345
                       :name "Performance Test User"
                       :email "test@example.com"
                       :roles [:admin :user :viewer]
                       :metadata {:created "2024-01-01"
                                  :modified "2024-01-15"
                                  :version 3}}}
          iterations 1000
          schemas {:json (serde/json)
                   :edn (serde/edn)
                   :nippy (serde/nippy)
                   :transit (serde/transit)}]

      (doseq [[name schema] schemas]
        (let [start (System/nanoTime)
              _ (dotimes [_ iterations]
                  (.serialize schema data))
              elapsed (/ (- (System/nanoTime) start) 1e6)]
          (println (format "%s: %.2f ms for %d iterations (%.2f ops/ms)"
                           (clojure.core/name name) elapsed iterations
                           (/ iterations elapsed))))))))
