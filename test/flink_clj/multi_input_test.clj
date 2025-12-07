(ns flink-clj.multi-input-test
  "Tests for multiple input operators."
  (:require [clojure.test :refer :all]
            [flink-clj.multi-input :as mi]
            [flink-clj.core :as flink]
            [flink-clj.env :as env]))

;; =============================================================================
;; API Structure Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "Connection functions exist"
    (is (fn? mi/connect-multiple))
    (is (fn? mi/stream-count))
    (is (fn? mi/get-stream)))

  (testing "Input selection functions exist"
    (is (fn? mi/input-selectable)))

  (testing "Pattern functions exist"
    (is (fn? mi/union-all))
    (is (fn? mi/cascading-connect)))

  (testing "Info functions exist"
    (is (fn? mi/multi-input-info))))

;; =============================================================================
;; Multi-Input Info Test
;; =============================================================================

(deftest multi-input-info-test
  (testing "multi-input-info returns proper structure"
    (let [info (mi/multi-input-info)]
      (is (map? info))
      (is (contains? info :available))
      (is (contains? info :patterns))
      (is (contains? info :notes))
      (is (boolean? (:available info))))))

;; =============================================================================
;; Union All Test
;; =============================================================================

(deftest union-all-test
  (testing "union-all requires at least one stream"
    (is (thrown? clojure.lang.ExceptionInfo
                 (mi/union-all []))))

  (testing "union-all with single stream returns that stream"
    (let [env (env/create-local-env {:parallelism 1})
          _ (flink/register-clojure-types! env)
          stream (flink/from-collection env [1 2 3])]
      (is (= stream (mi/union-all [stream]))))))

(deftest union-multiple-streams-test
  (testing "union-all combines multiple streams"
    (let [env (env/create-local-env {:parallelism 1})
          _ (flink/register-clojure-types! env)
          s1 (flink/from-collection env [1 2])
          s2 (flink/from-collection env [3 4])
          s3 (flink/from-collection env [5 6])
          unioned (mi/union-all [s1 s2 s3])]
      ;; Verify it's a DataStream
      (is (instance? org.apache.flink.streaming.api.datastream.DataStream unioned)))))

;; =============================================================================
;; Connect Multiple Test
;; =============================================================================

(deftest connect-multiple-validation-test
  (testing "connect-multiple requires at least 2 streams"
    (let [env (env/create-local-env {:parallelism 1})
          _ (flink/register-clojure-types! env)
          stream (flink/from-collection env [1 2 3])]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"At least 2 streams required"
            (mi/connect-multiple [stream]))))))

(deftest connect-multiple-structure-test
  (testing "connect-multiple creates proper structure"
    (let [env (env/create-local-env {:parallelism 1})
          _ (flink/register-clojure-types! env)
          s1 (flink/from-collection env [1 2])
          s2 (flink/from-collection env [3 4])
          s3 (flink/from-collection env [5 6])
          connected (mi/connect-multiple [s1 s2 s3])]
      (is (= :multiple-connected-streams (:type connected)))
      (is (= 3 (mi/stream-count connected)))
      (is (= s1 (mi/get-stream connected 0)))
      (is (= s2 (mi/get-stream connected 1)))
      (is (= s3 (mi/get-stream connected 2))))))

;; =============================================================================
;; Run tests
;; =============================================================================

(comment
  (run-tests))
