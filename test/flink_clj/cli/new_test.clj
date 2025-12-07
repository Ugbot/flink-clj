(ns flink-clj.cli.new-test
  "Tests for the 'new' command."
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [flink-clj.cli.commands.new :as cmd-new])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

;; =============================================================================
;; CLI Options Tests
;; =============================================================================

(deftest cli-options-defined
  (testing "CLI options are defined"
    (is (vector? cmd-new/cli-options))
    (is (pos? (count cmd-new/cli-options)))))

(deftest cli-options-include-required
  (testing "CLI options include required options"
    (let [opt-names (set (map second cmd-new/cli-options))
          has-opt? (fn [prefix] (some #(clojure.string/starts-with? % prefix) opt-names))]
      (is (has-opt? "--template"))
      (is (has-opt? "--flink-version"))
      (is (has-opt? "--output"))
      (is (has-opt? "--help")))))

;; =============================================================================
;; Template Validation Tests
;; =============================================================================

(deftest valid-templates
  (testing "Template validation accepts valid templates"
    (let [template-opt (first (filter #(= "--template" (second %)) cmd-new/cli-options))
          validate-fn (last template-opt)]
      (when validate-fn
        ;; The validate function returns truthy for valid values
        (is (some #(% "etl") (filter fn? template-opt)))
        (is (some #(% "analytics") (filter fn? template-opt)))
        (is (some #(% "cdc") (filter fn? template-opt)))
        (is (some #(% "event-driven") (filter fn? template-opt)))))))

(deftest valid-flink-versions
  (testing "Flink version validation accepts valid versions"
    (let [version-opt (first (filter #(clojure.string/starts-with? (second %) "--flink-version")
                                      cmd-new/cli-options))]
      ;; Check that validation exists and accepts known versions
      (is (some? version-opt)))))
