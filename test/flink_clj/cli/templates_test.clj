(ns flink-clj.cli.templates-test
  "Tests for project templates."
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [flink-clj.cli.templates.common :as tmpl])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

;; =============================================================================
;; Utility Functions
;; =============================================================================

(defn- create-temp-dir
  "Create a temporary directory for testing."
  []
  (let [path (Files/createTempDirectory "flink-clj-test"
               (into-array FileAttribute []))]
    (.toFile path)))

(defn- delete-dir
  "Recursively delete a directory."
  [^File dir]
  (when (.exists dir)
    (doseq [f (file-seq dir)]
      (.delete f))))

;; =============================================================================
;; Template Rendering Tests
;; =============================================================================

(deftest render-simple-substitution
  (testing "Simple variable substitution"
    (is (= "Hello World!"
           (tmpl/render "Hello {{name}}!" {:name "World"})))
    (is (= "Project: test-project"
           (tmpl/render "Project: {{project}}" {:project "test-project"})))))

(deftest render-multiple-substitutions
  (testing "Multiple variable substitutions"
    (is (= "Hello John, welcome to Flink"
           (tmpl/render "Hello {{name}}, welcome to {{product}}"
                        {:name "John" :product "Flink"})))))

(deftest render-no-substitution-if-missing
  (testing "Missing variables are left as-is"
    (is (= "Hello {{unknown}}!"
           (tmpl/render "Hello {{unknown}}!" {})))))

;; =============================================================================
;; Template Generation Tests
;; =============================================================================

(deftest project-clj-template-generates
  (testing "project.clj template generates valid content"
    (let [content (tmpl/project-clj-template {:name "test-project"
                                               :namespace "test_project"
                                               :flink-version "1.20"
                                               :template :etl})]
      (is (string? content))
      (is (str/includes? content "defproject"))
      (is (str/includes? content "test-project"))
      (is (str/includes? content "1.20")))))

(deftest deps-edn-template-generates
  (testing "deps.edn template generates valid content"
    (let [content (tmpl/deps-edn-template {:name "test-project"
                                            :namespace "test_project"
                                            :flink-version "1.20"
                                            :template :analytics})]
      (is (string? content))
      (is (str/includes? content ":deps"))
      (is (str/includes? content ":aliases")))))

(deftest user-clj-template-generates
  (testing "user.clj template generates valid content"
    (let [content (tmpl/user-clj-template {:namespace "my_project"})]
      (is (string? content))
      (is (str/includes? content "(ns user"))
      (is (str/includes? content "start!")))))

;; =============================================================================
;; Job Template Tests
;; =============================================================================

(deftest etl-template-generates
  (testing "ETL job template generates valid content"
    (let [content (tmpl/job-template {:name "etl-job"
                                       :namespace "etl_job"
                                       :template :etl})]
      (is (string? content))
      (is (str/includes? content "(ns etl_job.job"))
      (is (str/includes? content "ETL Pipeline"))
      (is (str/includes? content "parse-record"))
      (is (str/includes? content "-main")))))

(deftest analytics-template-generates
  (testing "Analytics job template generates valid content"
    (let [content (tmpl/job-template {:name "analytics-job"
                                       :namespace "analytics_job"
                                       :template :analytics})]
      (is (string? content))
      (is (str/includes? content "(ns analytics_job.job"))
      (is (str/includes? content "Stream Analytics"))
      (is (str/includes? content "aggregate")))))

(deftest cdc-template-generates
  (testing "CDC job template generates valid content"
    (let [content (tmpl/job-template {:name "cdc-job"
                                       :namespace "cdc_job"
                                       :template :cdc})]
      (is (string? content))
      (is (str/includes? content "(ns cdc_job.job"))
      (is (str/includes? content "CDC Processor"))
      (is (str/includes? content "process-change")))))

(deftest event-driven-template-generates
  (testing "Event-driven job template generates valid content"
    (let [content (tmpl/job-template {:name "event-job"
                                       :namespace "event_job"
                                       :template :event-driven})]
      (is (string? content))
      (is (str/includes? content "(ns event_job.job"))
      (is (str/includes? content "Event-Driven"))
      (is (str/includes? content "fraud-pattern")))))

;; =============================================================================
;; Project Generation Tests
;; =============================================================================

(deftest generate-project-creates-files
  (testing "generate-project! creates all required files"
    (let [temp-dir (create-temp-dir)
          project-dir (io/file temp-dir "test-project")]
      (try
        (.mkdirs project-dir)
        (.mkdirs (io/file project-dir "src" "test_project"))
        (.mkdirs (io/file project-dir "dev"))

        (tmpl/generate-project! project-dir
                                {:name "test-project"
                                 :namespace "test_project"
                                 :template :etl
                                 :flink-version "1.20"
                                 :deps-edn? false})

        (is (.exists (io/file project-dir "project.clj")))
        (is (.exists (io/file project-dir "src" "test_project" "job.clj")))
        (is (.exists (io/file project-dir "dev" "user.clj")))
        (is (.exists (io/file project-dir ".gitignore")))
        (is (.exists (io/file project-dir "README.md")))
        (finally
          (delete-dir temp-dir))))))

(deftest generate-project-with-deps-edn
  (testing "generate-project! creates deps.edn when requested"
    (let [temp-dir (create-temp-dir)
          project-dir (io/file temp-dir "deps-project")]
      (try
        (.mkdirs project-dir)
        (.mkdirs (io/file project-dir "src" "deps_project"))
        (.mkdirs (io/file project-dir "dev"))

        (tmpl/generate-project! project-dir
                                {:name "deps-project"
                                 :namespace "deps_project"
                                 :template :analytics
                                 :flink-version "2.x"
                                 :deps-edn? true})

        (is (.exists (io/file project-dir "deps.edn")))
        (is (not (.exists (io/file project-dir "project.clj"))))
        (finally
          (delete-dir temp-dir))))))

;; =============================================================================
;; Gitignore Template Tests
;; =============================================================================

(deftest gitignore-includes-common-patterns
  (testing "Gitignore includes common patterns"
    (is (str/includes? tmpl/gitignore-template "target/"))
    (is (str/includes? tmpl/gitignore-template ".nrepl-port"))
    (is (str/includes? tmpl/gitignore-template "*.jar"))
    (is (str/includes? tmpl/gitignore-template ".DS_Store"))))
