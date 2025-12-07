(ns flink-clj.ververica.client
  "HTTP client for Ververica Cloud Platform API.

   Provides low-level API access to Ververica Cloud services including:
   - Authentication (login, token management)
   - Workspace management
   - Deployment management
   - Job control (start, stop, status)
   - Savepoint management
   - Artifact management"
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.net HttpURLConnection URL]
           [java.io BufferedReader InputStreamReader OutputStreamWriter]
           [java.util Base64]))

;; =============================================================================
;; Configuration
;; =============================================================================

(def ^:dynamic *base-url* "https://app.ververica.cloud")
(def ^:dynamic *timeout-ms* 30000)

(defn config-file
  "Returns path to Ververica config file."
  []
  (str (System/getProperty "user.home") "/.flink-clj/ververica.json"))

(defn load-config
  "Load Ververica configuration from disk."
  []
  (let [f (io/file (config-file))]
    (when (.exists f)
      (json/read-str (slurp f) :key-fn keyword))))

(defn save-config!
  "Save Ververica configuration to disk."
  [config]
  (let [f (io/file (config-file))
        parent (.getParentFile f)]
    (when-not (.exists parent)
      (.mkdirs parent))
    (spit f (json/write-str config))
    config))

(defn get-token
  "Get current access token from config."
  []
  (:access-token (load-config)))

(defn get-workspace
  "Get current workspace from config."
  []
  (:workspace (load-config)))

;; =============================================================================
;; HTTP Client
;; =============================================================================

(defn- read-response
  "Read response body from connection."
  [^HttpURLConnection conn]
  (let [stream (try (.getInputStream conn)
                    (catch Exception _ (.getErrorStream conn)))]
    (when stream
      (with-open [reader (BufferedReader. (InputStreamReader. stream "UTF-8"))]
        (str/join "\n" (line-seq reader))))))

(defn- parse-response
  "Parse JSON response, returning nil for empty responses."
  [body]
  (when (and body (not (str/blank? body)))
    (try
      (json/read-str body :key-fn keyword)
      (catch Exception _
        {:raw-body body}))))

(defn request
  "Make HTTP request to Ververica API.

   Options:
   - :method - HTTP method (:get, :post, :put, :patch, :delete)
   - :path - API path (e.g., \"/api/v1/workspaces\")
   - :body - Request body (will be JSON encoded)
   - :token - Bearer token (optional, uses stored token if not provided)
   - :query-params - Map of query parameters
   - :headers - Additional headers"
  [{:keys [method path body token query-params headers]
    :or {method :get}}]
  (let [token (or token (get-token))
        url-str (str *base-url* path
                     (when (seq query-params)
                       (str "?" (str/join "&"
                                          (map (fn [[k v]]
                                                 (str (name k) "=" (java.net.URLEncoder/encode (str v) "UTF-8")))
                                               query-params)))))
        url (URL. url-str)
        conn ^HttpURLConnection (.openConnection url)]
    (try
      (.setRequestMethod conn (str/upper-case (name method)))
      (.setConnectTimeout conn *timeout-ms*)
      (.setReadTimeout conn *timeout-ms*)
      (.setRequestProperty conn "Content-Type" "application/json")
      (.setRequestProperty conn "Accept" "application/json")
      (when token
        (.setRequestProperty conn "Authorization" (str "Bearer " token)))
      (doseq [[k v] headers]
        (.setRequestProperty conn (name k) (str v)))

      (when body
        (.setDoOutput conn true)
        (with-open [writer (OutputStreamWriter. (.getOutputStream conn) "UTF-8")]
          (.write writer (json/write-str body))
          (.flush writer)))

      (let [status (.getResponseCode conn)
            response-body (read-response conn)
            parsed (parse-response response-body)]
        {:status status
         :body parsed
         :raw-body response-body
         :success? (< status 400)})
      (finally
        (.disconnect conn)))))

(defn api-get
  "GET request to API."
  [path & {:keys [token query-params]}]
  (request {:method :get
            :path path
            :token token
            :query-params query-params}))

(defn api-post
  "POST request to API."
  [path body & {:keys [token]}]
  (request {:method :post
            :path path
            :body body
            :token token}))

(defn api-put
  "PUT request to API."
  [path body & {:keys [token]}]
  (request {:method :put
            :path path
            :body body
            :token token}))

(defn api-patch
  "PATCH request to API."
  [path body & {:keys [token]}]
  (request {:method :patch
            :path path
            :body body
            :token token}))

(defn api-delete
  "DELETE request to API."
  [path & {:keys [token]}]
  (request {:method :delete
            :path path
            :token token}))

;; =============================================================================
;; Authentication
;; =============================================================================

(defn login
  "Authenticate with Ververica Cloud.

   Supports credential-based login:
   (login {:username \"user@example.com\" :password \"secret\"})

   Returns access token on success."
  [{:keys [username password]}]
  (let [response (api-post "/api/v1/auth/tokens"
                           {:flow "credentials"
                            :username username
                            :password password})]
    (if (:success? response)
      (let [body (:body response)
            config {:access-token (:accessToken body)
                    :refresh-token (:refreshToken body)
                    :user-id (:userId body)
                    :expires-at (:expiresAt body)}]
        (save-config! config)
        {:success true
         :message "Login successful"
         :user-id (:userId body)})
      {:success false
       :message (or (get-in response [:body :message])
                    (str "Login failed: " (:status response)))
       :error (:body response)})))

(defn logout
  "Clear stored credentials."
  []
  (let [f (io/file (config-file))]
    (when (.exists f)
      (.delete f)))
  {:success true :message "Logged out"})

(defn whoami
  "Get current user profile."
  []
  (let [response (api-get "/api/v1/users/profile")]
    (if (:success? response)
      {:success true :user (:body response)}
      {:success false :error (:body response)})))

;; =============================================================================
;; Workspaces
;; =============================================================================

(defn list-workspaces
  "List all workspaces accessible to the user."
  [& {:keys [status]}]
  (let [params (when status {:statusCategory status})
        response (api-get "/api/v1/workspaces" :query-params params)]
    (if (:success? response)
      {:success true :workspaces (get-in response [:body :workspaces])}
      {:success false :error (:body response)})))

(defn get-workspace-info
  "Get details of a specific workspace."
  [workspace-id]
  (let [response (api-get (str "/api/v1/workspaces/" workspace-id))]
    (if (:success? response)
      {:success true :workspace (:body response)}
      {:success false :error (:body response)})))

(defn create-workspace
  "Create a new workspace."
  [{:keys [name billing-plan-id cluster-id]}]
  (let [body (cond-> {:name name}
               billing-plan-id (assoc :billingPlanId billing-plan-id)
               cluster-id (assoc :clusterId cluster-id))
        response (api-post "/api/v1/workspaces" body)]
    (if (:success? response)
      {:success true :workspace (:body response)}
      {:success false :error (:body response)})))

(defn delete-workspace
  "Delete a workspace."
  [workspace-id]
  (let [response (api-delete (str "/api/v1/workspaces/" workspace-id))]
    (if (:success? response)
      {:success true :message "Workspace deleted"}
      {:success false :error (:body response)})))

(defn set-workspace!
  "Set the default workspace for CLI operations."
  [workspace-id]
  (let [config (or (load-config) {})]
    (save-config! (assoc config :workspace workspace-id))
    {:success true :workspace workspace-id}))

;; =============================================================================
;; Deployments
;; =============================================================================

(defn list-deployments
  "List deployments in a workspace."
  [workspace & {:keys [namespace] :or {namespace "default"}}]
  (let [path (str "/api/v1/workspaces/" workspace "/namespaces/" namespace "/deployments")
        response (api-get path)]
    (if (:success? response)
      {:success true :deployments (get-in response [:body :items] [])}
      {:success false :error (:body response)})))

(defn get-deployment
  "Get deployment details."
  [workspace deployment-id & {:keys [namespace] :or {namespace "default"}}]
  (let [path (str "/api/v1/workspaces/" workspace "/namespaces/" namespace "/deployments/" deployment-id)
        response (api-get path)]
    (if (:success? response)
      {:success true :deployment (:body response)}
      {:success false :error (:body response)})))

(defn create-deployment
  "Create a new deployment.

   Options:
   - :name - Deployment name (required)
   - :namespace - Namespace (default: \"default\")
   - :engine-version - Flink engine version
   - :jar-uri - URI to JAR file
   - :entry-class - Main class
   - :main-args - Arguments to main
   - :parallelism - Job parallelism
   - :deployment-target - Target name (for session clusters)"
  [workspace {:keys [name namespace engine-version jar-uri entry-class
                     main-args parallelism deployment-target execution-mode
                     flink-conf]
              :or {namespace "default"
                   execution-mode "STREAMING"
                   parallelism 1}}]
  (let [deployment {:name name
                    :namespace namespace
                    :executionMode execution-mode
                    :engineVersion engine-version
                    :artifact {:kind "JAR"
                               :jarArtifact (cond-> {:jarUri jar-uri}
                                              entry-class (assoc :entryClass entry-class)
                                              main-args (assoc :mainArgs main-args))}
                    :streamingResourceSetting {:resourceSettingMode "BASIC"
                                               :basicResourceSetting {:parallelism parallelism}}
                    :kerberosConfig {:kerberosEnabled false}}
        deployment (if deployment-target
                     (assoc deployment :deploymentTarget {:mode "SESSION"
                                                          :name deployment-target})
                     (assoc deployment :deploymentTarget {:mode "PER_JOB"
                                                          :name workspace}))
        deployment (if flink-conf
                     (assoc deployment :flinkConf flink-conf)
                     deployment)
        path (str "/api/v1/workspaces/" workspace "/namespaces/" namespace "/deployments")
        response (api-post path deployment)]
    (if (:success? response)
      {:success true :deployment (:body response)}
      {:success false :error (:body response)})))

(defn update-deployment
  "Update an existing deployment."
  [workspace deployment-id updates & {:keys [namespace] :or {namespace "default"}}]
  (let [path (str "/api/v1/workspaces/" workspace "/namespaces/" namespace "/deployments/" deployment-id)
        response (api-patch path updates)]
    (if (:success? response)
      {:success true :deployment (:body response)}
      {:success false :error (:body response)})))

(defn delete-deployment
  "Delete a deployment."
  [workspace deployment-id & {:keys [namespace] :or {namespace "default"}}]
  (let [path (str "/api/v1/workspaces/" workspace "/namespaces/" namespace "/deployments/" deployment-id)
        response (api-delete path)]
    (if (:success? response)
      {:success true :message "Deployment deleted"}
      {:success false :error (:body response)})))

;; =============================================================================
;; Jobs
;; =============================================================================

(defn list-jobs
  "List jobs in a workspace."
  [workspace & {:keys [namespace] :or {namespace "default"}}]
  (let [path (str "/api/v2/workspaces/" workspace "/namespaces/" namespace "/jobs")
        response (api-get path)]
    (if (:success? response)
      {:success true :jobs (get-in response [:body :items] [])}
      {:success false :error (:body response)})))

(defn get-job
  "Get job details."
  [workspace job-id & {:keys [namespace] :or {namespace "default"}}]
  (let [path (str "/api/v2/workspaces/" workspace "/namespaces/" namespace "/jobs/" job-id)
        response (api-get path)]
    (if (:success? response)
      {:success true :job (:body response)}
      {:success false :error (:body response)})))

(defn start-job
  "Start a job from a deployment.

   Options:
   - :deployment-id - ID of deployment to start
   - :restore-strategy - One of: NONE, LATEST_SAVEPOINT, LATEST_STATE, FROM_SAVEPOINT
   - :savepoint-id - Savepoint ID (when restore-strategy is FROM_SAVEPOINT)
   - :allow-non-restored-state - Allow starting without full state restoration"
  [workspace {:keys [deployment-id restore-strategy savepoint-id
                     allow-non-restored-state namespace]
              :or {namespace "default"
                   restore-strategy "NONE"}}]
  (let [body {:deploymentId deployment-id
              :restoreStrategy (cond-> {:kind restore-strategy}
                                 savepoint-id (assoc :savepointId savepoint-id)
                                 allow-non-restored-state (assoc :allowNonRestoredState true))}
        path (str "/api/v2/workspaces/" workspace "/namespaces/" namespace "/jobs:start")
        response (api-post path body)]
    (if (:success? response)
      {:success true :job (:body response)}
      {:success false :error (:body response)})))

(defn stop-job
  "Stop a running job.

   Options:
   - :kind - Stop kind: CANCEL, STOP_WITH_SAVEPOINT, SUSPEND
   - :savepoint-location - Location to store savepoint"
  [workspace job-id & {:keys [namespace kind savepoint-location]
                       :or {namespace "default"
                            kind "CANCEL"}}]
  (let [body (cond-> {:kind kind}
               savepoint-location (assoc :savepointLocation savepoint-location))
        path (str "/api/v2/workspaces/" workspace "/namespaces/" namespace "/jobs/" job-id ":stop")
        response (api-post path body)]
    (if (:success? response)
      {:success true :job (:body response)}
      {:success false :error (:body response)})))

;; =============================================================================
;; Savepoints
;; =============================================================================

(defn list-savepoints
  "List savepoints for a deployment."
  [workspace deployment-id & {:keys [namespace] :or {namespace "default"}}]
  (let [path (str "/api/v1/workspaces/" workspace "/namespaces/" namespace
                  "/deployments/" deployment-id "/savepoints")
        response (api-get path)]
    (if (:success? response)
      {:success true :savepoints (get-in response [:body :items] [])}
      {:success false :error (:body response)})))

(defn create-savepoint
  "Trigger a savepoint for a deployment."
  [workspace deployment-id & {:keys [namespace] :or {namespace "default"}}]
  (let [path (str "/api/v1/workspaces/" workspace "/namespaces/" namespace
                  "/deployments/" deployment-id "/savepoints")
        response (api-post path {})]
    (if (:success? response)
      {:success true :savepoint (:body response)}
      {:success false :error (:body response)})))

(defn delete-savepoint
  "Delete a savepoint."
  [workspace deployment-id savepoint-id & {:keys [namespace] :or {namespace "default"}}]
  (let [path (str "/api/v1/workspaces/" workspace "/namespaces/" namespace
                  "/deployments/" deployment-id "/savepoints/" savepoint-id)
        response (api-delete path)]
    (if (:success? response)
      {:success true :message "Savepoint deleted"}
      {:success false :error (:body response)})))

;; =============================================================================
;; Artifacts
;; =============================================================================

(defn list-artifacts
  "List artifacts in a workspace."
  [workspace & {:keys [namespace] :or {namespace "default"}}]
  (let [path (str "/api/v1/workspaces/" workspace "/namespaces/" namespace "/artifacts")
        response (api-get path)]
    (if (:success? response)
      {:success true :artifacts (get-in response [:body :items] [])}
      {:success false :error (:body response)})))

(defn upload-artifact
  "Upload a JAR artifact to Ververica.

   Note: This generates a signed upload URL and uploads the file."
  [workspace file-path & {:keys [namespace] :or {namespace "default"}}]
  (let [file (io/file file-path)
        filename (.getName file)]
    (if-not (.exists file)
      {:success false :error {:message (str "File not found: " file-path)}}
      (let [;; Get upload signature
            sig-path (str "/api/v1/workspaces/" workspace "/namespaces/" namespace
                          "/artifacts/" filename "/signature")
            sig-response (api-get sig-path)]
        (if-not (:success? sig-response)
          {:success false :error (:body sig-response)}
          (let [upload-url (get-in sig-response [:body :url])
                ;; Upload file to signed URL
                url (URL. upload-url)
                conn ^HttpURLConnection (.openConnection url)]
            (try
              (.setRequestMethod conn "PUT")
              (.setDoOutput conn true)
              (.setRequestProperty conn "Content-Type" "application/java-archive")
              (with-open [out (.getOutputStream conn)
                          in (io/input-stream file)]
                (io/copy in out))
              (let [status (.getResponseCode conn)]
                (if (< status 400)
                  {:success true
                   :artifact {:name filename
                              :uri (str "s3i://vvc-tenant-" workspace "-*/artifacts/namespaces/"
                                        namespace "/" filename)}}
                  {:success false :error {:message (str "Upload failed: " status)}}))
              (finally
                (.disconnect conn)))))))))

;; =============================================================================
;; Session Clusters
;; =============================================================================

(defn list-session-clusters
  "List session clusters in a workspace."
  [workspace & {:keys [namespace] :or {namespace "default"}}]
  (let [path (str "/api/v1/workspaces/" workspace "/namespaces/" namespace "/sessionclusters")
        response (api-get path)]
    (if (:success? response)
      {:success true :session-clusters (get-in response [:body :items] [])}
      {:success false :error (:body response)})))

(defn create-session-cluster
  "Create a new session cluster."
  [workspace {:keys [name namespace engine-version]
              :or {namespace "default"}}]
  (let [body {:name name
              :namespace namespace
              :engineVersion engine-version}
        path (str "/api/v1/workspaces/" workspace "/namespaces/" namespace "/sessionclusters")
        response (api-post path body)]
    (if (:success? response)
      {:success true :session-cluster (:body response)}
      {:success false :error (:body response)})))

;; =============================================================================
;; Engine Versions
;; =============================================================================

(defn list-engine-versions
  "List available Flink engine versions."
  [workspace]
  (let [path (str "/api/v2/workspaces/" workspace "/flink/engine-version-meta.json")
        response (api-get path)]
    (if (:success? response)
      {:success true :versions (:body response)}
      {:success false :error (:body response)})))

;; =============================================================================
;; Regions & Billing
;; =============================================================================

(defn list-regions
  "List available regions."
  []
  (let [response (api-get "/api/v1/regions")]
    (if (:success? response)
      {:success true :regions (get-in response [:body :regions] [])}
      {:success false :error (:body response)})))

(defn list-billing-plans
  "List available billing plans."
  []
  (let [response (api-get "/api/v1/billing/plans")]
    (if (:success? response)
      {:success true :plans (get-in response [:body :plans] [])}
      {:success false :error (:body response)})))
