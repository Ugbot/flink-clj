(ns flink-clj.cli.core-test
  "Tests for CLI core functionality."
  (:require [clojure.test :refer :all]
            [flink-clj.cli.core :as cli]))

;; =============================================================================
;; Version and Commands Tests
;; =============================================================================

(deftest version-defined
  (testing "Version is defined"
    (is (string? cli/version))
    (is (re-matches #"\d+\.\d+\.\d+" cli/version))))

(deftest commands-defined
  (testing "All required commands are defined"
    (is (contains? cli/commands "new"))
    (is (contains? cli/commands "repl"))
    (is (contains? cli/commands "run"))
    (is (contains? cli/commands "build"))
    (is (contains? cli/commands "deploy"))
    (is (contains? cli/commands "jobs"))))

(deftest command-structure
  (testing "Each command has required keys"
    (doseq [[cmd-name cmd-spec] cli/commands]
      (testing (str "Command: " cmd-name)
        (is (contains? cmd-spec :desc) "Missing :desc")
        (is (contains? cmd-spec :fn) "Missing :fn")
        (is (string? (:desc cmd-spec)) ":desc should be a string")
        (is (symbol? (:fn cmd-spec)) ":fn should be a symbol")))))

;; =============================================================================
;; CLI Options Tests
;; =============================================================================

(deftest cli-options-valid
  (testing "CLI options are properly structured"
    (is (vector? cli/cli-options))
    (doseq [opt cli/cli-options]
      (is (vector? opt))
      (is (>= (count opt) 3)))))
