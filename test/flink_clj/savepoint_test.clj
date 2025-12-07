(ns flink-clj.savepoint-test
  "Tests for savepoint management utilities."
  (:require [clojure.test :refer :all]
            [flink-clj.savepoint :as sp]))

;; =============================================================================
;; API Structure Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "Savepoint trigger functions exist"
    (is (fn? sp/trigger))
    (is (fn? sp/trigger-sync))
    (is (fn? sp/stop-with-savepoint))
    (is (fn? sp/stop-with-savepoint-sync)))

  (testing "Job status functions exist"
    (is (fn? sp/job-id))
    (is (fn? sp/job-status))
    (is (fn? sp/job-status-sync)))

  (testing "Job control functions exist"
    (is (fn? sp/cancel))
    (is (fn? sp/cancel-sync))
    (is (fn? sp/job-result))
    (is (fn? sp/await-termination)))

  (testing "Restore functions exist"
    (is (fn? sp/with-savepoint)))

  (testing "Utility functions exist"
    (is (fn? sp/savepoint-info))
    (is (fn? sp/wait-until-running))
    (is (fn? sp/ensure-job-running))))

;; =============================================================================
;; Integration tests would require a running Flink cluster
;; These are marked as integration tests
;; =============================================================================

(deftest ^:integration savepoint-trigger-test
  (testing "Savepoint operations require JobClient"
    ;; This test would require an actual JobClient from a running job
    ;; Placeholder for integration test
    (is true)))

;; =============================================================================
;; Run tests
;; =============================================================================

(comment
  (run-tests))
