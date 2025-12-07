(ns flink-clj.ververica-test
  "Tests for Ververica Cloud integration."
  (:require [clojure.test :refer :all]
            [flink-clj.ververica.client :as vvc]
            [clojure.java.io :as io]))

;; =============================================================================
;; Configuration Tests
;; =============================================================================

(deftest config-file-test
  (testing "config file path is in user home"
    (let [path (vvc/config-file)]
      (is (string? path))
      (is (.contains path ".flink-clj"))
      (is (.contains path "ververica.json")))))

(deftest load-config-test
  (testing "load-config returns nil when no config exists"
    (let [test-home (str (System/getProperty "java.io.tmpdir") "/test-flink-clj-" (rand-int 10000))]
      (with-redefs [vvc/config-file (fn [] (str test-home "/ververica.json"))]
        (is (nil? (vvc/load-config)))))))

(deftest save-config-test
  (testing "save and load config round-trips"
    (let [test-home (str (System/getProperty "java.io.tmpdir") "/test-flink-clj-" (rand-int 10000))
          config-path (str test-home "/ververica.json")
          test-config {:access-token "test-token"
                       :workspace "test-workspace"}]
      (try
        (with-redefs [vvc/config-file (fn [] config-path)]
          (vvc/save-config! test-config)
          (is (.exists (io/file config-path)))
          (let [loaded (vvc/load-config)]
            (is (= "test-token" (:access-token loaded)))
            (is (= "test-workspace" (:workspace loaded)))))
        (finally
          ;; Cleanup
          (io/delete-file config-path true)
          (io/delete-file test-home true))))))

;; =============================================================================
;; Request Building Tests
;; =============================================================================

(deftest request-structure-test
  (testing "request builds correct structure"
    (let [captured (atom nil)]
      (with-redefs [vvc/request (fn [opts]
                                  (reset! captured opts)
                                  {:status 200 :success? true :body {}})]
        (vvc/api-get "/test/path" :query-params {:foo "bar"})
        (is (= :get (:method @captured)))
        (is (= "/test/path" (:path @captured)))
        (is (= {:foo "bar"} (:query-params @captured)))))))

(deftest api-post-test
  (testing "api-post sends correct body"
    (let [captured (atom nil)]
      (with-redefs [vvc/request (fn [opts]
                                  (reset! captured opts)
                                  {:status 200 :success? true :body {}})]
        (vvc/api-post "/test/path" {:key "value"})
        (is (= :post (:method @captured)))
        (is (= {:key "value"} (:body @captured)))))))

;; =============================================================================
;; Response Parsing Tests
;; =============================================================================

(deftest login-response-parsing-test
  (testing "login parses successful response"
    (let [test-home (str (System/getProperty "java.io.tmpdir") "/test-flink-clj-" (rand-int 10000))
          config-path (str test-home "/ververica.json")]
      (try
        (with-redefs [vvc/config-file (fn [] config-path)
                      vvc/request (fn [_]
                                    {:status 200
                                     :success? true
                                     :body {:accessToken "abc123"
                                            :refreshToken "refresh123"
                                            :userId "user-001"
                                            :expiresAt "2025-01-01T00:00:00Z"}})]
          (let [result (vvc/login {:username "test@test.com" :password "secret"})]
            (is (:success result))
            (is (= "user-001" (:user-id result)))))
        (finally
          (io/delete-file config-path true)
          (io/delete-file test-home true))))))

(deftest login-failure-test
  (testing "login returns error on failure"
    (with-redefs [vvc/request (fn [_]
                                {:status 401
                                 :success? false
                                 :body {:message "Invalid credentials"}})]
      (let [result (vvc/login {:username "test@test.com" :password "wrong"})]
        (is (not (:success result)))
        (is (= "Invalid credentials" (:message result)))))))

;; =============================================================================
;; Workspace Tests
;; =============================================================================

(deftest list-workspaces-test
  (testing "list-workspaces parses response"
    (with-redefs [vvc/get-token (fn [] "token")
                  vvc/request (fn [_]
                                {:status 200
                                 :success? true
                                 :body {:workspaces [{:instanceId "ws-001"
                                                      :name "test-workspace"
                                                      :status {:name "Running"}}]}})]
      (let [result (vvc/list-workspaces)]
        (is (:success result))
        (is (= 1 (count (:workspaces result))))
        (is (= "ws-001" (get-in result [:workspaces 0 :instanceId])))))))

(deftest set-workspace-test
  (testing "set-workspace stores workspace id"
    (let [test-home (str (System/getProperty "java.io.tmpdir") "/test-flink-clj-" (rand-int 10000))
          config-path (str test-home "/ververica.json")]
      (try
        (with-redefs [vvc/config-file (fn [] config-path)]
          (vvc/set-workspace! "my-workspace")
          (is (= "my-workspace" (vvc/get-workspace))))
        (finally
          (io/delete-file config-path true)
          (io/delete-file test-home true))))))

;; =============================================================================
;; Deployment Tests
;; =============================================================================

(deftest list-deployments-test
  (testing "list-deployments parses response"
    (with-redefs [vvc/get-token (fn [] "token")
                  vvc/request (fn [opts]
                                (is (.contains (:path opts) "/deployments"))
                                {:status 200
                                 :success? true
                                 :body {:items [{:deploymentId "dep-001"
                                                 :name "my-deployment"
                                                 :engineVersion "vera-1.0.3-flink-1.17"}]}})]
      (let [result (vvc/list-deployments "ws-001")]
        (is (:success result))
        (is (= 1 (count (:deployments result))))))))

(deftest create-deployment-test
  (testing "create-deployment builds correct request"
    (let [captured (atom nil)]
      (with-redefs [vvc/get-token (fn [] "token")
                    vvc/request (fn [opts]
                                  (reset! captured (:body opts))
                                  {:status 200
                                   :success? true
                                   :body {:deploymentId "dep-new"}})]
        (vvc/create-deployment "ws-001"
                               {:name "test-job"
                                :jar-uri "s3://bucket/job.jar"
                                :entry-class "com.example.Main"
                                :parallelism 4})
        (is (= "test-job" (:name @captured)))
        (is (= "JAR" (get-in @captured [:artifact :kind])))
        (is (= "s3://bucket/job.jar" (get-in @captured [:artifact :jarArtifact :jarUri])))
        (is (= "com.example.Main" (get-in @captured [:artifact :jarArtifact :entryClass])))
        (is (= 4 (get-in @captured [:streamingResourceSetting :basicResourceSetting :parallelism])))))))

;; =============================================================================
;; Job Tests
;; =============================================================================

(deftest start-job-test
  (testing "start-job builds correct request"
    (let [captured (atom nil)]
      (with-redefs [vvc/get-token (fn [] "token")
                    vvc/request (fn [opts]
                                  (reset! captured (:body opts))
                                  {:status 200
                                   :success? true
                                   :body {:metadata {:id "job-001"}}})]
        (vvc/start-job "ws-001"
                       {:deployment-id "dep-001"
                        :restore-strategy "LATEST_SAVEPOINT"})
        (is (= "dep-001" (:deploymentId @captured)))
        (is (= "LATEST_SAVEPOINT" (get-in @captured [:restoreStrategy :kind])))))))

(deftest stop-job-test
  (testing "stop-job builds correct request"
    (let [captured (atom nil)]
      (with-redefs [vvc/get-token (fn [] "token")
                    vvc/request (fn [opts]
                                  (reset! captured opts)
                                  {:status 200
                                   :success? true
                                   :body {}})]
        (vvc/stop-job "ws-001" "job-001" :kind "STOP_WITH_SAVEPOINT")
        (is (.contains (:path @captured) "job-001:stop"))
        (is (= "STOP_WITH_SAVEPOINT" (get-in @captured [:body :kind])))))))

;; =============================================================================
;; Savepoint Tests
;; =============================================================================

(deftest list-savepoints-test
  (testing "list-savepoints parses response"
    (with-redefs [vvc/get-token (fn [] "token")
                  vvc/request (fn [opts]
                                (is (.contains (:path opts) "/savepoints"))
                                {:status 200
                                 :success? true
                                 :body {:items [{:metadata {:id "sp-001"}
                                                 :status {:state "COMPLETED"}}]}})]
      (let [result (vvc/list-savepoints "ws-001" "dep-001")]
        (is (:success result))
        (is (= 1 (count (:savepoints result))))))))

(deftest create-savepoint-test
  (testing "create-savepoint sends request"
    (let [captured (atom nil)]
      (with-redefs [vvc/get-token (fn [] "token")
                    vvc/request (fn [opts]
                                  (reset! captured opts)
                                  {:status 200
                                   :success? true
                                   :body {:metadata {:id "sp-new"}}})]
        (vvc/create-savepoint "ws-001" "dep-001")
        (is (= :post (:method @captured)))
        (is (.contains (:path @captured) "/savepoints"))))))

;; =============================================================================
;; CLI Command Tests
;; =============================================================================

(deftest ververica-command-registered-test
  (testing "ververica command is registered in CLI"
    (require 'flink-clj.cli.core)
    (let [commands @(resolve 'flink-clj.cli.core/commands)]
      (is (contains? commands "ververica"))
      (is (contains? commands "vvc")))))
