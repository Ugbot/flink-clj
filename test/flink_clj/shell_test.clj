(ns flink-clj.shell-test
  "Tests for the interactive shell."
  (:require [clojure.test :refer :all]
            [flink-clj.shell :as shell]
            [flink-clj.env :as env]
            [flink-clj.core :as flink]))

;; =============================================================================
;; Shell State Tests
;; =============================================================================

(deftest shell-not-running-initially
  (testing "Shell is not running before start!"
    (let [status (shell/status)]
      (is (false? (:running? status))))))

;; =============================================================================
;; Local Mode Tests
;; =============================================================================

(deftest local-mode-start-stop
  (testing "Start and stop in local mode"
    ;; Start
    (shell/start! {:parallelism 2})
    (let [status (shell/status)]
      (is (true? (:running? status)))
      (is (= :local (:mode status)))
      (is (= 2 (:parallelism status))))

    ;; Verify environments are bound
    (is (some? shell/env))
    (is (instance? org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
                   shell/env))

    ;; Stop
    (shell/stop!)
    (let [status (shell/status)]
      (is (false? (:running? status)))
      (is (nil? (:mode status))))

    ;; Verify environments are cleared
    (is (nil? shell/env))))

(deftest local-mode-with-web-ui
  (testing "Start with web UI option"
    (shell/start! {:parallelism 1 :web-ui true :web-ui-port 18081})
    (let [status (shell/status)]
      (is (= "http://localhost:18081" (:web-ui-url status))))
    (is (= "http://localhost:18081" (shell/web-ui-url)))
    (shell/stop!)))

(deftest restart-with-new-options
  (testing "Restart changes configuration"
    (shell/start! {:parallelism 1})
    (is (= 1 (:parallelism (shell/status))))

    (shell/restart! {:parallelism 4})
    (is (= 4 (:parallelism (shell/status))))

    (shell/stop!)))

(deftest double-start-is-safe
  (testing "Starting when already running shows message"
    (shell/start!)
    (let [status-before (shell/status)]
      (shell/start!)  ; Should print message, not throw
      (is (= status-before (shell/status))))
    (shell/stop!)))

(deftest double-stop-is-safe
  (testing "Stopping when not running shows message"
    (shell/stop!)  ; Should print message, not throw
    (is (false? (:running? (shell/status))))))

;; =============================================================================
;; Collection Tests
;; =============================================================================

(deftest collect-results-test
  (testing "Collect results from stream"
    (shell/start! {:parallelism 1})
    (let [stream (flink/from-collection shell/env [1 2 3 4 5])
          results (shell/collect stream)]
      (is (= [1 2 3 4 5] (sort results))))
    (shell/stop!)))

(deftest take-n-results-test
  (testing "Take first n results from stream"
    (shell/start! {:parallelism 1})
    (let [stream (flink/from-collection shell/env [1 2 3 4 5])
          results (shell/take-n stream 3)]
      (is (= 3 (count results))))
    (shell/stop!)))

;; =============================================================================
;; Introspection Tests
;; =============================================================================

(deftest desc-stream-test
  (testing "Describe a stream"
    (shell/start! {:parallelism 2})
    (let [stream (flink/from-collection shell/env [1 2 3])
          description (shell/desc stream)]
      (is (map? description))
      (is (contains? description :type))
      (is (contains? description :parallelism))
      (is (contains? description :name)))
    (shell/stop!)))

(deftest help-runs-without-error
  (testing "Help function runs"
    (shell/start!)
    (is (nil? (shell/help)))  ; Just verify it doesn't throw
    (shell/stop!)))

;; =============================================================================
;; Error Handling Tests
;; =============================================================================

(deftest collect-requires-running-shell
  (testing "Collect throws when shell not running"
    (shell/stop!)  ; Ensure stopped
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Shell not running"
                          (shell/collect nil)))))

(deftest jobs-requires-remote-mode
  (testing "Jobs function requires remote mode"
    (shell/start! {:mode :local})
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"remote mode"
                          (shell/jobs)))
    (shell/stop!)))

;; =============================================================================
;; Remote Environment Creation Tests
;; =============================================================================

(deftest create-remote-env-test
  (testing "Create remote environment (no actual connection)"
    ;; This just tests that the function constructs correctly
    ;; Actual connection would fail without a running cluster
    (is (instance? org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
                   (env/create-remote-env "localhost" 8081)))))

(deftest create-remote-env-with-options
  (testing "Create remote environment with options"
    (let [remote-env (env/create-remote-env "localhost" 8081 {:parallelism 4})]
      (is (= 4 (.getParallelism remote-env))))))

;; =============================================================================
;; Cleanup
;; =============================================================================

(use-fixtures :each
  (fn [f]
    ;; Ensure clean state before each test
    (when (:running? (shell/status))
      (shell/stop!))
    (f)
    ;; Cleanup after test
    (when (:running? (shell/status))
      (shell/stop!))))
