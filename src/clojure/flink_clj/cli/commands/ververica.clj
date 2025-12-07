(ns flink-clj.cli.commands.ververica
  "CLI commands for Ververica Cloud Platform.

   Provides commands for managing Flink deployments on Ververica Cloud:
   - Authentication (login, logout, whoami)
   - Workspace management (list, use, create, delete)
   - Deployment management (list, create, delete, describe)
   - Job control (list, start, stop, status)
   - Savepoint management (list, create, delete)
   - Artifact management (list, upload)"
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as str]
            [flink-clj.ververica.client :as vvc]))

;; =============================================================================
;; Output Formatting
;; =============================================================================

(defn- print-table
  "Print data as a formatted table."
  [headers rows]
  (when (seq rows)
    (let [col-widths (map (fn [i]
                            (apply max
                                   (count (nth headers i))
                                   (map #(count (str (nth % i ""))) rows)))
                          (range (count headers)))
          fmt-str (str/join " | " (map #(str "%-" % "s") col-widths))
          separator (str/join "-+-" (map #(apply str (repeat % "-")) col-widths))]
      (println (apply format fmt-str headers))
      (println separator)
      (doseq [row rows]
        (println (apply format fmt-str (map str row)))))))

(defn- print-json
  "Print data as JSON."
  [data]
  (println (clojure.data.json/write-str data :indent true)))

(defn- print-result
  "Print command result with optional JSON output."
  [result {:keys [json]}]
  (if json
    (print-json result)
    (if (:success result)
      (println (:message result "OK"))
      (do
        (println "Error:" (or (get-in result [:error :message])
                              (str (:error result))))
        (System/exit 1)))))

;; =============================================================================
;; Login Command
;; =============================================================================

(def login-options
  [["-u" "--username EMAIL" "Email address"]
   ["-p" "--password PASSWORD" "Password (will prompt if not provided)"]
   ["-h" "--help" "Show help"]])

(defn- read-password
  "Read password from console without echoing."
  []
  (if-let [console (System/console)]
    (String. (.readPassword console "Password: " (into-array Object [])))
    (do
      (print "Password: ")
      (flush)
      (read-line))))

(defn cmd-login
  "Authenticate with Ververica Cloud."
  [args]
  (let [{:keys [options errors summary]} (parse-opts args login-options)]
    (cond
      (:help options)
      (do
        (println "Usage: flink-clj ververica login [options]")
        (println)
        (println "Authenticate with Ververica Cloud Platform.")
        (println)
        (println "Options:")
        (println summary))

      errors
      (do
        (doseq [e errors] (println "Error:" e))
        (System/exit 1))

      :else
      (let [username (or (:username options)
                         (do (print "Email: ") (flush) (read-line)))
            password (or (:password options) (read-password))
            result (vvc/login {:username username :password password})]
        (if (:success result)
          (println "Logged in as" (:user-id result))
          (do
            (println "Login failed:" (:message result))
            (System/exit 1)))))))

(defn cmd-logout
  "Clear stored credentials."
  [_args]
  (vvc/logout)
  (println "Logged out"))

(defn cmd-whoami
  "Show current user."
  [args]
  (let [{:keys [options]} (parse-opts args [["-j" "--json" "Output as JSON"]])
        result (vvc/whoami)]
    (if (:success result)
      (if (:json options)
        (print-json (:user result))
        (let [user (:user result)]
          (println "User:" (or (:fullName user) (:email user)))
          (println "Email:" (:email user))
          (println "ID:" (:id user))))
      (do
        (println "Error:" (or (get-in result [:error :message]) "Not logged in"))
        (System/exit 1)))))

;; =============================================================================
;; Workspace Commands
;; =============================================================================

(def workspace-list-options
  [["-s" "--status STATUS" "Filter by status (OK, PROCESSING, ERROR)"]
   ["-j" "--json" "Output as JSON"]
   ["-h" "--help" "Show help"]])

(defn cmd-workspace-list
  "List workspaces."
  [args]
  (let [{:keys [options errors summary]} (parse-opts args workspace-list-options)]
    (cond
      (:help options)
      (do
        (println "Usage: flink-clj ververica workspace list [options]")
        (println)
        (println "Options:")
        (println summary))

      errors
      (do (doseq [e errors] (println "Error:" e)) (System/exit 1))

      :else
      (let [result (vvc/list-workspaces :status (:status options))]
        (if (:success result)
          (if (:json options)
            (print-json (:workspaces result))
            (print-table ["ID" "NAME" "STATUS" "REGION" "CU USED" "CU TOTAL"]
                         (map (fn [w]
                                [(:instanceId w)
                                 (:name w)
                                 (get-in w [:status :name] "?")
                                 (:regionId w)
                                 (get-in w [:computeUnits :used] "-")
                                 (get-in w [:computeUnits :total] "-")])
                              (:workspaces result))))
          (do
            (println "Error:" (get-in result [:error :message]))
            (System/exit 1)))))))

(defn cmd-workspace-use
  "Set default workspace."
  [args]
  (if (empty? args)
    (do
      (println "Usage: flink-clj ververica workspace use <workspace-id>")
      (System/exit 1))
    (let [workspace-id (first args)
          result (vvc/set-workspace! workspace-id)]
      (println "Using workspace:" workspace-id))))

(defn cmd-workspace-info
  "Show workspace details."
  [args]
  (let [{:keys [options]} (parse-opts args [["-j" "--json" "Output as JSON"]])
        workspace-id (or (first args) (vvc/get-workspace))]
    (if-not workspace-id
      (do
        (println "Error: No workspace specified. Use 'ververica workspace use <id>' first.")
        (System/exit 1))
      (let [result (vvc/get-workspace-info workspace-id)]
        (if (:success result)
          (if (:json options)
            (print-json (:workspace result))
            (let [w (:workspace result)]
              (println "Workspace:" (:name w))
              (println "ID:" (:instanceId w))
              (println "Status:" (get-in w [:status :name]))
              (println "Region:" (:regionId w))
              (println "Offering:" (:offeringType w))
              (println "Console:" (:consoleUri w))
              (when-let [cu (:computeUnits w)]
                (println "Compute Units:" (:used cu) "/" (:total cu)))))
          (do
            (println "Error:" (get-in result [:error :message]))
            (System/exit 1)))))))

(defn cmd-workspace
  "Workspace management commands."
  [args]
  (let [subcommand (first args)
        subargs (rest args)]
    (case subcommand
      "list" (cmd-workspace-list subargs)
      "ls" (cmd-workspace-list subargs)
      "use" (cmd-workspace-use subargs)
      "info" (cmd-workspace-info subargs)
      "show" (cmd-workspace-info subargs)
      (nil "") (do
                 (println "Usage: flink-clj ververica workspace <command>")
                 (println)
                 (println "Commands:")
                 (println "  list    List workspaces")
                 (println "  use     Set default workspace")
                 (println "  info    Show workspace details"))
      (do
        (println "Unknown workspace command:" subcommand)
        (System/exit 1)))))

;; =============================================================================
;; Deployment Commands
;; =============================================================================

(def deployment-list-options
  [["-n" "--namespace NS" "Namespace" :default "default"]
   ["-j" "--json" "Output as JSON"]
   ["-h" "--help" "Show help"]])

(defn cmd-deployment-list
  "List deployments."
  [args]
  (let [{:keys [options errors summary]} (parse-opts args deployment-list-options)
        workspace (vvc/get-workspace)]
    (cond
      (:help options)
      (do
        (println "Usage: flink-clj ververica deployment list [options]")
        (println)
        (println "Options:")
        (println summary))

      errors
      (do (doseq [e errors] (println "Error:" e)) (System/exit 1))

      (not workspace)
      (do
        (println "Error: No workspace set. Use 'ververica workspace use <id>' first.")
        (System/exit 1))

      :else
      (let [result (vvc/list-deployments workspace :namespace (:namespace options))]
        (if (:success result)
          (if (:json options)
            (print-json (:deployments result))
            (print-table ["ID" "NAME" "STATE" "ENGINE" "PARALLELISM"]
                         (map (fn [d]
                                [(subs (str (:deploymentId d)) 0 8)
                                 (:name d)
                                 (get-in d [:jobSummary :state] "UNKNOWN")
                                 (:engineVersion d)
                                 (get-in d [:streamingResourceSetting :basicResourceSetting :parallelism] "-")])
                              (:deployments result))))
          (do
            (println "Error:" (get-in result [:error :message]))
            (System/exit 1)))))))

(def deployment-create-options
  [[nil "--name NAME" "Deployment name" :required "NAME"]
   [nil "--jar URI" "JAR URI (local path or remote URL)" :required "JAR"]
   [nil "--entry-class CLASS" "Entry class name"]
   [nil "--args ARGS" "Arguments to main method"]
   [nil "--engine VERSION" "Flink engine version"]
   [nil "--parallelism N" "Job parallelism" :default 1 :parse-fn #(Integer/parseInt %)]
   [nil "--session-cluster NAME" "Deploy to session cluster"]
   ["-n" "--namespace NS" "Namespace" :default "default"]
   ["-j" "--json" "Output as JSON"]
   ["-h" "--help" "Show help"]])

(defn cmd-deployment-create
  "Create a new deployment."
  [args]
  (let [{:keys [options errors summary]} (parse-opts args deployment-create-options)
        workspace (vvc/get-workspace)]
    (cond
      (:help options)
      (do
        (println "Usage: flink-clj ververica deployment create [options]")
        (println)
        (println "Create a new Flink deployment on Ververica Cloud.")
        (println)
        (println "Options:")
        (println summary)
        (println)
        (println "Examples:")
        (println "  flink-clj ververica deployment create --name my-job --jar target/my-job.jar")
        (println "  flink-clj ververica deployment create --name my-job --jar s3://bucket/job.jar --entry-class com.example.Main"))

      errors
      (do (doseq [e errors] (println "Error:" e)) (System/exit 1))

      (not workspace)
      (do
        (println "Error: No workspace set. Use 'ververica workspace use <id>' first.")
        (System/exit 1))

      (not (:name options))
      (do (println "Error: --name is required") (System/exit 1))

      (not (:jar options))
      (do (println "Error: --jar is required") (System/exit 1))

      :else
      (let [;; If jar is a local file, upload it first
            jar-uri (if (.exists (clojure.java.io/file (:jar options)))
                      (let [upload-result (vvc/upload-artifact workspace (:jar options)
                                                               :namespace (:namespace options))]
                        (if (:success upload-result)
                          (do
                            (println "Uploaded:" (:jar options))
                            (get-in upload-result [:artifact :uri]))
                          (do
                            (println "Upload failed:" (get-in upload-result [:error :message]))
                            (System/exit 1))))
                      (:jar options))
            result (vvc/create-deployment workspace
                                          {:name (:name options)
                                           :namespace (:namespace options)
                                           :jar-uri jar-uri
                                           :entry-class (:entry-class options)
                                           :main-args (:args options)
                                           :engine-version (:engine options)
                                           :parallelism (:parallelism options)
                                           :deployment-target (:session-cluster options)})]
        (if (:success result)
          (if (:json options)
            (print-json (:deployment result))
            (do
              (println "Created deployment:" (:name options))
              (println "ID:" (get-in result [:deployment :deploymentId]))))
          (do
            (println "Error:" (get-in result [:error :message]))
            (System/exit 1)))))))

(defn cmd-deployment-delete
  "Delete a deployment."
  [args]
  (let [{:keys [options]} (parse-opts args [["-n" "--namespace NS" :default "default"]])
        workspace (vvc/get-workspace)
        deployment-id (first args)]
    (cond
      (not workspace)
      (do
        (println "Error: No workspace set. Use 'ververica workspace use <id>' first.")
        (System/exit 1))

      (not deployment-id)
      (do (println "Usage: flink-clj ververica deployment delete <deployment-id>") (System/exit 1))

      :else
      (let [result (vvc/delete-deployment workspace deployment-id :namespace (:namespace options))]
        (if (:success result)
          (println "Deleted deployment:" deployment-id)
          (do
            (println "Error:" (get-in result [:error :message]))
            (System/exit 1)))))))

(defn cmd-deployment-describe
  "Show deployment details."
  [args]
  (let [{:keys [options]} (parse-opts args [["-n" "--namespace NS" :default "default"]
                                            ["-j" "--json" "Output as JSON"]])
        workspace (vvc/get-workspace)
        deployment-id (first args)]
    (cond
      (not workspace)
      (do
        (println "Error: No workspace set.")
        (System/exit 1))

      (not deployment-id)
      (do (println "Usage: flink-clj ververica deployment describe <deployment-id>") (System/exit 1))

      :else
      (let [result (vvc/get-deployment workspace deployment-id :namespace (:namespace options))]
        (if (:success result)
          (if (:json options)
            (print-json (:deployment result))
            (let [d (:deployment result)]
              (println "Name:" (:name d))
              (println "ID:" (:deploymentId d))
              (println "State:" (get-in d [:jobSummary :state] "UNKNOWN"))
              (println "Engine:" (:engineVersion d))
              (println "Mode:" (:executionMode d))
              (println "Parallelism:" (get-in d [:streamingResourceSetting :basicResourceSetting :parallelism]))
              (println "Created:" (:createdAt d))
              (println "Modified:" (:modifiedAt d))))
          (do
            (println "Error:" (get-in result [:error :message]))
            (System/exit 1)))))))

(defn cmd-deployment
  "Deployment management commands."
  [args]
  (let [subcommand (first args)
        subargs (rest args)]
    (case subcommand
      "list" (cmd-deployment-list subargs)
      "ls" (cmd-deployment-list subargs)
      "create" (cmd-deployment-create subargs)
      "delete" (cmd-deployment-delete subargs)
      "rm" (cmd-deployment-delete subargs)
      "describe" (cmd-deployment-describe subargs)
      "show" (cmd-deployment-describe subargs)
      (nil "") (do
                 (println "Usage: flink-clj ververica deployment <command>")
                 (println)
                 (println "Commands:")
                 (println "  list      List deployments")
                 (println "  create    Create a new deployment")
                 (println "  delete    Delete a deployment")
                 (println "  describe  Show deployment details"))
      (do
        (println "Unknown deployment command:" subcommand)
        (System/exit 1)))))

;; =============================================================================
;; Job Commands
;; =============================================================================

(def job-list-options
  [["-n" "--namespace NS" "Namespace" :default "default"]
   ["-j" "--json" "Output as JSON"]
   ["-h" "--help" "Show help"]])

(defn cmd-job-list
  "List jobs."
  [args]
  (let [{:keys [options errors summary]} (parse-opts args job-list-options)
        workspace (vvc/get-workspace)]
    (cond
      (:help options)
      (do
        (println "Usage: flink-clj ververica job list [options]")
        (println)
        (println "Options:")
        (println summary))

      errors
      (do (doseq [e errors] (println "Error:" e)) (System/exit 1))

      (not workspace)
      (do
        (println "Error: No workspace set.")
        (System/exit 1))

      :else
      (let [result (vvc/list-jobs workspace :namespace (:namespace options))]
        (if (:success result)
          (if (:json options)
            (print-json (:jobs result))
            (print-table ["JOB ID" "DEPLOYMENT" "STATE" "START TIME"]
                         (map (fn [j]
                                [(subs (str (get-in j [:metadata :id])) 0 8)
                                 (get-in j [:metadata :name] "-")
                                 (get-in j [:status :state] "?")
                                 (get-in j [:status :startTime] "-")])
                              (:jobs result))))
          (do
            (println "Error:" (get-in result [:error :message]))
            (System/exit 1)))))))

(def job-start-options
  [["-d" "--deployment ID" "Deployment ID" :required "ID"]
   ["-r" "--restore STRATEGY" "Restore strategy (NONE, LATEST_SAVEPOINT, LATEST_STATE)"
    :default "NONE"]
   ["-s" "--savepoint ID" "Savepoint ID (for FROM_SAVEPOINT)"]
   [nil "--allow-non-restored" "Allow non-restored state"]
   ["-n" "--namespace NS" "Namespace" :default "default"]
   ["-j" "--json" "Output as JSON"]
   ["-h" "--help" "Show help"]])

(defn cmd-job-start
  "Start a job."
  [args]
  (let [{:keys [options errors summary]} (parse-opts args job-start-options)
        workspace (vvc/get-workspace)]
    (cond
      (:help options)
      (do
        (println "Usage: flink-clj ververica job start [options]")
        (println)
        (println "Start a Flink job from an existing deployment.")
        (println)
        (println "Options:")
        (println summary)
        (println)
        (println "Restore strategies:")
        (println "  NONE              Start fresh without state")
        (println "  LATEST_SAVEPOINT  Restore from most recent savepoint")
        (println "  LATEST_STATE      Restore from most recent state")
        (println "  FROM_SAVEPOINT    Restore from specific savepoint (requires --savepoint)"))

      errors
      (do (doseq [e errors] (println "Error:" e)) (System/exit 1))

      (not workspace)
      (do (println "Error: No workspace set.") (System/exit 1))

      (not (:deployment options))
      (do (println "Error: --deployment is required") (System/exit 1))

      :else
      (let [result (vvc/start-job workspace
                                  {:deployment-id (:deployment options)
                                   :namespace (:namespace options)
                                   :restore-strategy (:restore options)
                                   :savepoint-id (:savepoint options)
                                   :allow-non-restored-state (:allow-non-restored options)})]
        (if (:success result)
          (if (:json options)
            (print-json (:job result))
            (do
              (println "Job started")
              (println "Job ID:" (get-in result [:job :metadata :id]))))
          (do
            (println "Error:" (get-in result [:error :message]))
            (System/exit 1)))))))

(def job-stop-options
  [["-k" "--kind KIND" "Stop kind (CANCEL, STOP_WITH_SAVEPOINT, SUSPEND)" :default "CANCEL"]
   ["-n" "--namespace NS" "Namespace" :default "default"]
   ["-j" "--json" "Output as JSON"]
   ["-h" "--help" "Show help"]])

(defn cmd-job-stop
  "Stop a job."
  [args]
  (let [{:keys [options errors summary]} (parse-opts args job-stop-options)
        workspace (vvc/get-workspace)
        job-id (first args)]
    (cond
      (:help options)
      (do
        (println "Usage: flink-clj ververica job stop <job-id> [options]")
        (println)
        (println "Options:")
        (println summary)
        (println)
        (println "Stop kinds:")
        (println "  CANCEL              Cancel immediately (no savepoint)")
        (println "  STOP_WITH_SAVEPOINT Stop gracefully with savepoint")
        (println "  SUSPEND             Suspend with savepoint"))

      errors
      (do (doseq [e errors] (println "Error:" e)) (System/exit 1))

      (not workspace)
      (do (println "Error: No workspace set.") (System/exit 1))

      (not job-id)
      (do (println "Usage: flink-clj ververica job stop <job-id>") (System/exit 1))

      :else
      (let [result (vvc/stop-job workspace job-id
                                 :namespace (:namespace options)
                                 :kind (:kind options))]
        (if (:success result)
          (if (:json options)
            (print-json (:job result))
            (println "Job stopped:" job-id))
          (do
            (println "Error:" (get-in result [:error :message]))
            (System/exit 1)))))))

(defn cmd-job-status
  "Show job status."
  [args]
  (let [{:keys [options]} (parse-opts args [["-n" "--namespace NS" :default "default"]
                                            ["-j" "--json" "Output as JSON"]])
        workspace (vvc/get-workspace)
        job-id (first args)]
    (cond
      (not workspace)
      (do (println "Error: No workspace set.") (System/exit 1))

      (not job-id)
      (do (println "Usage: flink-clj ververica job status <job-id>") (System/exit 1))

      :else
      (let [result (vvc/get-job workspace job-id :namespace (:namespace options))]
        (if (:success result)
          (if (:json options)
            (print-json (:job result))
            (let [j (:job result)]
              (println "Job ID:" (get-in j [:metadata :id]))
              (println "Name:" (get-in j [:metadata :name]))
              (println "State:" (get-in j [:status :state]))
              (println "Start Time:" (get-in j [:status :startTime]))
              (println "Flink Job ID:" (get-in j [:status :flinkJobId]))))
          (do
            (println "Error:" (get-in result [:error :message]))
            (System/exit 1)))))))

(defn cmd-job
  "Job management commands."
  [args]
  (let [subcommand (first args)
        subargs (rest args)]
    (case subcommand
      "list" (cmd-job-list subargs)
      "ls" (cmd-job-list subargs)
      "start" (cmd-job-start subargs)
      "stop" (cmd-job-stop subargs)
      "status" (cmd-job-status subargs)
      (nil "") (do
                 (println "Usage: flink-clj ververica job <command>")
                 (println)
                 (println "Commands:")
                 (println "  list    List jobs")
                 (println "  start   Start a job from deployment")
                 (println "  stop    Stop a running job")
                 (println "  status  Show job status"))
      (do
        (println "Unknown job command:" subcommand)
        (System/exit 1)))))

;; =============================================================================
;; Savepoint Commands
;; =============================================================================

(defn cmd-savepoint-list
  "List savepoints."
  [args]
  (let [{:keys [options]} (parse-opts args [["-n" "--namespace NS" :default "default"]
                                            ["-j" "--json" "Output as JSON"]])
        workspace (vvc/get-workspace)
        deployment-id (first args)]
    (cond
      (not workspace)
      (do (println "Error: No workspace set.") (System/exit 1))

      (not deployment-id)
      (do (println "Usage: flink-clj ververica savepoint list <deployment-id>") (System/exit 1))

      :else
      (let [result (vvc/list-savepoints workspace deployment-id :namespace (:namespace options))]
        (if (:success result)
          (if (:json options)
            (print-json (:savepoints result))
            (print-table ["ID" "STATE" "CREATED" "TRIGGER"]
                         (map (fn [s]
                                [(subs (str (get-in s [:metadata :id])) 0 8)
                                 (get-in s [:status :state] "?")
                                 (get-in s [:metadata :createdAt] "-")
                                 (get-in s [:spec :triggerType] "-")])
                              (:savepoints result))))
          (do
            (println "Error:" (get-in result [:error :message]))
            (System/exit 1)))))))

(defn cmd-savepoint-create
  "Create a savepoint."
  [args]
  (let [{:keys [options]} (parse-opts args [["-n" "--namespace NS" :default "default"]])
        workspace (vvc/get-workspace)
        deployment-id (first args)]
    (cond
      (not workspace)
      (do (println "Error: No workspace set.") (System/exit 1))

      (not deployment-id)
      (do (println "Usage: flink-clj ververica savepoint create <deployment-id>") (System/exit 1))

      :else
      (let [result (vvc/create-savepoint workspace deployment-id :namespace (:namespace options))]
        (if (:success result)
          (do
            (println "Savepoint triggered")
            (println "ID:" (get-in result [:savepoint :metadata :id])))
          (do
            (println "Error:" (get-in result [:error :message]))
            (System/exit 1)))))))

(defn cmd-savepoint
  "Savepoint management commands."
  [args]
  (let [subcommand (first args)
        subargs (rest args)]
    (case subcommand
      "list" (cmd-savepoint-list subargs)
      "ls" (cmd-savepoint-list subargs)
      "create" (cmd-savepoint-create subargs)
      "trigger" (cmd-savepoint-create subargs)
      (nil "") (do
                 (println "Usage: flink-clj ververica savepoint <command> <deployment-id>")
                 (println)
                 (println "Commands:")
                 (println "  list    List savepoints for a deployment")
                 (println "  create  Trigger a new savepoint"))
      (do
        (println "Unknown savepoint command:" subcommand)
        (System/exit 1)))))

;; =============================================================================
;; Artifact Commands
;; =============================================================================

(defn cmd-artifact-list
  "List artifacts."
  [args]
  (let [{:keys [options]} (parse-opts args [["-n" "--namespace NS" :default "default"]
                                            ["-j" "--json" "Output as JSON"]])
        workspace (vvc/get-workspace)]
    (if (not workspace)
      (do (println "Error: No workspace set.") (System/exit 1))
      (let [result (vvc/list-artifacts workspace :namespace (:namespace options))]
        (if (:success result)
          (if (:json options)
            (print-json (:artifacts result))
            (print-table ["NAME" "TYPE" "SIZE"]
                         (map (fn [a]
                                [(:name a) (:type a "JAR") (:size a "-")])
                              (:artifacts result))))
          (do
            (println "Error:" (get-in result [:error :message]))
            (System/exit 1)))))))

(defn cmd-artifact-upload
  "Upload an artifact."
  [args]
  (let [{:keys [options]} (parse-opts args [["-n" "--namespace NS" :default "default"]])
        workspace (vvc/get-workspace)
        file-path (first args)]
    (cond
      (not workspace)
      (do (println "Error: No workspace set.") (System/exit 1))

      (not file-path)
      (do (println "Usage: flink-clj ververica artifact upload <file>") (System/exit 1))

      :else
      (do
        (println "Uploading" file-path "...")
        (let [result (vvc/upload-artifact workspace file-path :namespace (:namespace options))]
          (if (:success result)
            (do
              (println "Uploaded:" (get-in result [:artifact :name]))
              (println "URI:" (get-in result [:artifact :uri])))
            (do
              (println "Error:" (get-in result [:error :message]))
              (System/exit 1))))))))

(defn cmd-artifact
  "Artifact management commands."
  [args]
  (let [subcommand (first args)
        subargs (rest args)]
    (case subcommand
      "list" (cmd-artifact-list subargs)
      "ls" (cmd-artifact-list subargs)
      "upload" (cmd-artifact-upload subargs)
      (nil "") (do
                 (println "Usage: flink-clj ververica artifact <command>")
                 (println)
                 (println "Commands:")
                 (println "  list    List artifacts")
                 (println "  upload  Upload a JAR artifact"))
      (do
        (println "Unknown artifact command:" subcommand)
        (System/exit 1)))))

;; =============================================================================
;; Main Entry Point
;; =============================================================================

(def cli-options
  [["-h" "--help" "Show help"]])

(defn show-help
  "Show main help for ververica command."
  []
  (println "flink-clj ververica - Manage Flink deployments on Ververica Cloud")
  (println)
  (println "Usage: flink-clj ververica <command> [options]")
  (println)
  (println "Authentication:")
  (println "  login       Authenticate with Ververica Cloud")
  (println "  logout      Clear stored credentials")
  (println "  whoami      Show current user")
  (println)
  (println "Resources:")
  (println "  workspace   Manage workspaces")
  (println "  deployment  Manage deployments")
  (println "  job         Manage jobs (start, stop, status)")
  (println "  savepoint   Manage savepoints")
  (println "  artifact    Manage artifacts")
  (println)
  (println "Examples:")
  (println "  flink-clj ververica login")
  (println "  flink-clj ververica workspace list")
  (println "  flink-clj ververica workspace use <workspace-id>")
  (println "  flink-clj ververica deployment create --name my-job --jar target/my-job.jar")
  (println "  flink-clj ververica job start --deployment <id>")
  (println "  flink-clj ververica job stop <job-id>")
  (println)
  (println "Run 'flink-clj ververica <command> --help' for more information on a command."))

(defn run
  "Main entry point for ververica command."
  [args]
  (let [{:keys [options]} (parse-opts args cli-options :in-order true)
        command (first args)
        subargs (rest args)]
    (if (or (:help options) (nil? command))
      (show-help)
      (case command
        "login" (cmd-login subargs)
        "logout" (cmd-logout subargs)
        "whoami" (cmd-whoami subargs)
        "workspace" (cmd-workspace subargs)
        "ws" (cmd-workspace subargs)
        "deployment" (cmd-deployment subargs)
        "deploy" (cmd-deployment subargs)
        "job" (cmd-job subargs)
        "savepoint" (cmd-savepoint subargs)
        "sp" (cmd-savepoint subargs)
        "artifact" (cmd-artifact subargs)
        "help" (show-help)
        (do
          (println "Unknown command:" command)
          (println)
          (show-help)
          (System/exit 1))))))
