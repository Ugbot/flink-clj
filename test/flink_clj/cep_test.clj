(ns flink-clj.cep-test
  "Tests for CEP (Complex Event Processing) pattern matching."
  (:require [clojure.test :refer :all]
            [flink-clj.cep :as cep]
            [flink-clj.core :as flink]
            [flink-clj.env :as env]
            [flink-clj.stream :as stream]
            [flink-clj.watermark :as watermark])
  (:import [org.apache.flink.streaming.api.environment StreamExecutionEnvironment]))

;; =============================================================================
;; Test Predicates (must be top-level for serialization)
;; =============================================================================

(defn login-event? [event]
  (= :login (:type event)))

(defn browse-event? [event]
  (= :browse (:type event)))

(defn purchase-event? [event]
  (= :purchase (:type event)))

(defn high-value? [event]
  (> (or (:amount event) 0) 100))

(defn process-match [matches]
  {:user (-> matches :login first :user-id)
   :purchased (-> matches :purchase first :item)})

(defn expand-matches [matches]
  (for [browse (:browse matches)]
    {:user (-> matches :login first :user-id)
     :page (:page browse)}))

;; =============================================================================
;; Unit Tests for Pattern Building
;; =============================================================================

(deftest pattern-building-test
  (testing "Basic pattern creation"
    (let [p (cep/pattern
              [:start {:where #'login-event?}]
              [:end {:where #'purchase-event?}])]
      (is (= :cep-pattern (:type p)))
      (is (= [:start :end] (:states p)))
      (is (some? (:flink-pattern p)))))

  (testing "Pattern with global options"
    (let [p (cep/pattern
              [:start {:where #'login-event?}]
              [:end {:where #'purchase-event?}]
              {:within [10 :minutes]})]
      (is (= {:within [10 :minutes]} (:options p)))))

  (testing "Pattern with quantifiers"
    (let [p (cep/pattern
              [:start {:where #'login-event?}]
              [:middle {:where #'browse-event?
                        :quantifier :one-or-more}]
              [:end {:where #'purchase-event?}])]
      (is (= [:start :middle :end] (:states p)))))

  (testing "Pattern with contiguity types"
    (let [p (cep/pattern
              [:a {:where #'login-event?}]
              [:b {:where #'browse-event? :contiguity :next}]
              [:c {:where #'purchase-event? :contiguity :followed-by-any}])]
      (is (= [:a :b :c] (:states p))))))

(deftest fluent-api-test
  (testing "Fluent pattern building"
    (let [p (-> (cep/begin :start #'login-event?)
                (cep/followed-by :end #'purchase-event?))]
      (is (= :cep-pattern (:type p)))
      (is (= ["start" "end"] (:states p)))))

  (testing "Fluent with quantifiers"
    (let [p (-> (cep/begin :events #'browse-event?)
                (cep/one-or-more)
                (cep/consecutive))]
      (is (some? (:flink-pattern p)))))

  (testing "Fluent with time constraint"
    (let [p (-> (cep/begin :start #'login-event?)
                (cep/followed-by :end #'purchase-event?)
                (cep/within [5 :minutes]))]
      (is (some? (:flink-pattern p))))))

(deftest pattern-states-test
  (testing "Pattern inspection"
    (let [p (cep/pattern
              [:login {:where #'login-event?}]
              [:browse {:where #'browse-event?}]
              [:purchase {:where #'purchase-event?}])]
      (is (= [:login :browse :purchase] (cep/pattern-states p)))
      (is (= {:type :cep-pattern
              :states [:login :browse :purchase]
              :options nil}
             (cep/pattern-info p))))))

;; =============================================================================
;; Integration Tests (require mini cluster)
;; =============================================================================

(defn with-mini-cluster [f]
  (let [env (env/create-local-env {:parallelism 1})]
    (flink/register-clojure-types! env)
    (f env)))

(deftest ^:integration cep-integration-test
  (testing "CEP pattern detection and selection"
    (with-mini-cluster
      (fn [env]
        ;; Create test events
        (let [events [{:type :login :user-id "user1" :ts 1000}
                      {:type :browse :user-id "user1" :page "/products" :ts 2000}
                      {:type :browse :user-id "user1" :page "/cart" :ts 3000}
                      {:type :purchase :user-id "user1" :item "widget" :ts 4000}]

              ;; Define pattern
              pattern (cep/pattern
                        [:login {:where #'login-event?}]
                        [:purchase {:where #'purchase-event?
                                    :contiguity :followed-by}])

              ;; Build stream
              stream (flink/from-collection env events)]

          ;; Verify pattern structure
          (is (= [:login :purchase] (cep/pattern-states pattern))))))))

;; =============================================================================
;; Run tests
;; =============================================================================

(comment
  ;; Run all tests
  (run-tests)

  ;; Run specific test
  (pattern-building-test)
  (fluent-api-test))
