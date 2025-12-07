(ns flink-clj.dsl-test
  "Tests for the idiomatic DSL namespace."
  (:require [clojure.test :refer :all]
            [flink-clj.dsl :as f]
            [flink-clj.core :as flink]
            [flink-clj.test-utils :refer [with-mini-cluster collect-results]]))

;; Test functions (top-level for serialization)
(defn double-it [x] (* x 2))
(defn triple-it [x] (* x 3))
(defn even-val? [x] (even? x))
(defn pos-val? [x] (pos? x))
(defn to-pair [x] [x 1])
(defn sum-pairs [[k1 v1] [_ v2]] [k1 (+ v1 v2)])

;; =============================================================================
;; resolve-fn tests
;; =============================================================================

(deftest resolve-fn-var-test
  (testing "resolve-fn returns var as-is"
    (let [result (#'f/resolve-fn #'double-it)]
      (is (var? result)))))

(deftest resolve-fn-keyword-test
  (testing "resolve-fn returns keyword as-is"
    (let [result (#'f/resolve-fn :user-id)]
      (is (= :user-id result)))))

;; =============================================================================
;; Pipeline composition tests
;; =============================================================================

(deftest pipeline-composition-test
  (testing "pipeline composes operations in order"
    (let [transform (f/pipeline
                      (f/map #'double-it)
                      (f/map #'triple-it))]
      (is (fn? transform)))))

;; =============================================================================
;; Window specs tests
;; =============================================================================

(deftest tumbling-window-spec-test
  (testing "tumbling creates correct spec"
    (let [spec (f/tumbling 10 :seconds)]
      (is (= :tumbling (:type spec)))
      (is (some? (:size spec))))))

(deftest sliding-window-spec-test
  (testing "sliding creates correct spec"
    (let [spec (f/sliding 1 :minutes 10 :seconds)]
      (is (= :sliding (:type spec)))
      (is (some? (:size spec)))
      (is (some? (:slide spec))))))

(deftest session-window-spec-test
  (testing "session creates correct spec"
    (let [spec (f/session 5 :minutes)]
      (is (= :session (:type spec)))
      (is (some? (:gap spec))))))

;; =============================================================================
;; Source specs tests
;; =============================================================================

(deftest collection-spec-test
  (testing "collection creates correct spec"
    (let [spec (f/collection [1 2 3])]
      (is (= :collection (:type spec)))
      (is (= [1 2 3] (:data spec))))))

(deftest kafka-source-spec-test
  (testing "kafka-source creates correct spec"
    (let [spec (f/kafka-source {:servers "localhost:9092"
                                :topic "events"
                                :group "my-group"})]
      (is (= :kafka (:type spec)))
      (is (= "localhost:9092" (get-in spec [:config :bootstrap-servers])))
      (is (= ["events"] (get-in spec [:config :topics])))
      (is (= "my-group" (get-in spec [:config :group-id]))))))

;; =============================================================================
;; Integration tests (require mini cluster)
;; =============================================================================

(deftest dsl-map-test
  (with-mini-cluster [env]
    (testing "DSL map works"
      (let [result (-> (flink/from-collection env [1 2 3 4 5])
                       (f/map #'double-it)
                       collect-results)]
        (is (= #{2 4 6 8 10} (set result)))))))

(deftest dsl-filter-test
  (with-mini-cluster [env]
    (testing "DSL filter works"
      (let [result (-> (flink/from-collection env [1 2 3 4 5 6])
                       (f/filter #'even-val?)
                       collect-results)]
        (is (= #{2 4 6} (set result)))))))

(deftest dsl-pipeline-integration-test
  (with-mini-cluster [env]
    (testing "DSL pipeline composition works"
      (let [transform (f/pipeline
                        (f/map #'double-it)
                        (f/filter #'pos-val?))
            result (-> (flink/from-collection env [1 2 3])
                       transform
                       collect-results)]
        (is (= #{2 4 6} (set result)))))))

(deftest dsl-key-by-test
  (with-mini-cluster [env]
    (testing "DSL key-by works"
      (let [result (-> (flink/from-collection env [1 2 3])
                       (f/map #'to-pair)
                       (f/key-by #'clojure.core/first {:key-type :long})
                       collect-results)]
        (is (= 3 (count result)))))))

(deftest dsl-key-by-reduce-test
  (with-mini-cluster [env]
    (testing "DSL key-by and reduce work together"
      (let [result (-> (flink/from-collection env [1 1 2 2 2 3])
                       (f/map #'to-pair)
                       (f/key-by #'clojure.core/first {:key-type :long})
                       (f/reduce #'sum-pairs)
                       collect-results)
            ;; Get final values for each key
            by-key (group-by first result)
            final-counts (into {} (for [[k vs] by-key]
                                    [k (apply max (map second vs))]))]
        (is (= {1 2, 2 3, 3 1} final-counts))))))

;; =============================================================================
;; Conditional pipeline tests
;; =============================================================================

(deftest when-macro-true-test
  (testing "when-> applies op when condition is true"
    (let [should-filter? true]
      (with-mini-cluster [env]
        (let [result (-> (flink/from-collection env [1 2 3 4 5 6])
                         (f/when-> should-filter?
                           (f/filter #'even-val?))
                         collect-results)]
          (is (= #{2 4 6} (set result))))))))

(deftest when-macro-false-test
  (testing "when-> skips op when condition is false"
    (let [should-filter? false]
      (with-mini-cluster [env]
        (let [result (-> (flink/from-collection env [1 2 3 4 5 6])
                         (f/when-> should-filter?
                           (f/filter #'even-val?))
                         collect-results)]
          (is (= #{1 2 3 4 5 6} (set result))))))))

(deftest if-macro-test
  (testing "if-> branches correctly"
    (with-mini-cluster [env]
      (let [use-double? true
            result (-> (flink/from-collection env [1 2 3])
                       (f/if-> use-double?
                         (f/map #'double-it)
                         (f/map #'triple-it))
                       collect-results)]
        (is (= #{2 4 6} (set result)))))))

;; =============================================================================
;; Count Window specs tests
;; =============================================================================

(deftest count-tumbling-window-spec-test
  (testing "count-tumbling creates correct spec"
    (let [spec (f/count-tumbling 100)]
      (is (= :count-tumbling (:type spec)))
      (is (= 100 (:size spec))))))

(deftest count-sliding-window-spec-test
  (testing "count-sliding creates correct spec"
    (let [spec (f/count-sliding 100 10)]
      (is (= :count-sliding (:type spec)))
      (is (= 100 (:size spec)))
      (is (= 10 (:slide spec))))))

;; =============================================================================
;; Dynamic Session Window specs tests
;; =============================================================================

(defn test-gap-fn [x] 30000)

(deftest dynamic-session-window-spec-test
  (testing "dynamic-session creates correct spec"
    (let [spec (f/dynamic-session test-gap-fn)]
      (is (= :dynamic-session (:type spec)))
      (is (= test-gap-fn (:gap-fn spec))))))

;; =============================================================================
;; Hybrid Source specs tests
;; =============================================================================

(deftest hybrid-source-spec-test
  (testing "hybrid-source creates correct spec"
    (let [bounded-src "bounded-placeholder"
          unbounded-src "unbounded-placeholder"
          spec (f/hybrid-source {:bounded bounded-src
                                 :unbounded unbounded-src})]
      (is (= :hybrid (:type spec)))
      (is (= bounded-src (get-in spec [:config :bounded])))
      (is (= unbounded-src (get-in spec [:config :unbounded]))))))
