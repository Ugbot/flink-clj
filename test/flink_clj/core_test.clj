(ns flink-clj.core-test
  "Tests for flink-clj.core namespace."
  (:require [clojure.test :refer :all]
            [flink-clj.core :as flink]
            [flink-clj.env :as env]
            [flink-clj.stream :as stream]
            [flink-clj.keyed :as keyed]
            [flink-clj.impl.functions :as impl]))

;; Test functions (must be defined at top level for serialization)
(defn double-value [x] (* x 2))
(defn even-value? [x] (clojure.core/even? x))
(defn add-one [x] (inc x))

(deftest var->ns-name-test
  (testing "Extracts namespace and name from var"
    (let [[ns-str fn-name] (impl/var->ns-name #'double-value)]
      (is (= "flink-clj.core-test" ns-str))
      (is (= "double-value" fn-name)))))

(deftest fn->var-throws-for-anonymous-test
  (testing "Throws helpful error for anonymous functions"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Anonymous functions cannot be serialized"
          (impl/fn->var (fn [x] x))))))

(deftest create-env-test
  (testing "Creates environment with default settings"
    (let [env (env/create-env)]
      (is (some? env))))

  (testing "Creates environment with custom parallelism"
    (let [env (env/create-env {:parallelism 4})]
      (is (= 4 (.getParallelism env))))))

(deftest from-collection-test
  (testing "Creates DataStream from collection"
    (let [env (env/create-env {:parallelism 1})
          stream (flink/from-collection env [1 2 3])]
      (is (some? stream)))))

;; Integration tests (require MiniCluster, run separately)
(deftest ^:integration simple-pipeline-test
  (testing "Simple map pipeline executes"
    (let [env (env/create-local-env {:parallelism 1})]
      (flink/register-clojure-types! env)
      (-> (flink/from-collection env [1 2 3 4 5])
          (stream/flink-map #'double-value)
          (stream/flink-print))
      ;; Just verify it doesn't throw
      (is (some? (flink/execute env "Test"))))))
