(ns flink-clj.cli.commands.jobs
  "Manage Flink cluster jobs.

  Usage:
    flink-clj jobs [subcommand] [options]

  Subcommands:
    list    List all jobs (default)
    info    Get job details
    cancel  Cancel a running job"
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as str]
            [flink-clj.rest :as rest])
  (:import [java.text SimpleDateFormat]
           [java.util Date]))

;; =============================================================================
;; CLI Options
;; =============================================================================

(def cli-options
  [["-H" "--host HOST" "Flink cluster host"
    :default "localhost"]
   ["-p" "--port PORT" "Flink cluster REST port"
    :default 8081
    :parse-fn #(Integer/parseInt %)]
   ["-s" "--status STATUS" "Filter by status (running, finished, failed, canceled)"
    :validate [#(contains? #{"running" "finished" "failed" "canceled" "all"} (str/lower-case %))
               "Must be: running, finished, failed, canceled, or all"]]
   ["-f" "--format FORMAT" "Output format (table, json)"
    :default "table"
    :validate [#(contains? #{"table" "json"} %)
               "Must be: table or json"]]
   ["-h" "--help" "Show help"]])

;; =============================================================================
;; Help
;; =============================================================================

(defn- print-help [summary]
  (println "flink-clj jobs - Manage Flink cluster jobs")
  (println)
  (println "Usage:")
  (println "  flink-clj jobs [subcommand] [options]")
  (println)
  (println "Subcommands:")
  (println "  list              List all jobs (default)")
  (println "  info <job-id>     Get detailed job information")
  (println "  cancel <job-id>   Cancel a running job")
  (println)
  (println "Options:")
  (println summary)
  (println)
  (println "Examples:")
  (println "  flink-clj jobs")
  (println "  flink-clj jobs list --status running")
  (println "  flink-clj jobs info abc123...")
  (println "  flink-clj jobs cancel abc123...")
  (println "  flink-clj jobs --host flink-cluster --port 8081"))

;; =============================================================================
;; Formatting
;; =============================================================================

(def ^:private date-formatter (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss"))

(defn- format-timestamp
  "Format timestamp to human-readable date."
  [ts]
  (if (and ts (pos? ts))
    (.format date-formatter (Date. ts))
    "-"))

(defn- format-duration
  "Format duration in milliseconds to human-readable string."
  [ms]
  (if (and ms (pos? ms))
    (let [seconds (quot ms 1000)
          minutes (quot seconds 60)
          hours (quot minutes 60)
          days (quot hours 24)]
      (cond
        (pos? days) (format "%dd %dh" days (rem hours 24))
        (pos? hours) (format "%dh %dm" hours (rem minutes 60))
        (pos? minutes) (format "%dm %ds" minutes (rem seconds 60))
        :else (format "%ds" seconds)))
    "-"))

(defn- truncate
  "Truncate string to max length."
  [s max-len]
  (if (> (count s) max-len)
    (str (subs s 0 (- max-len 3)) "...")
    s))

(defn- print-table
  "Print data as formatted table."
  [headers rows]
  (let [widths (map-indexed
                 (fn [i _]
                   (apply max (cons (count (nth headers i))
                                    (map #(count (str (nth % i ""))) rows))))
                 headers)
        format-row (fn [row]
                     (str/join " | "
                       (map-indexed
                         (fn [i cell]
                           (format (str "%-" (nth widths i) "s") (str cell)))
                         row)))]
    (println (format-row headers))
    (println (str/join "-+-" (map #(apply str (repeat % "-")) widths)))
    (doseq [row rows]
      (println (format-row row)))))

;; =============================================================================
;; Commands
;; =============================================================================

(defn- connect-to-cluster
  "Connect to Flink cluster."
  [host port]
  (let [base-url (format "http://%s:%d" host port)]
    (try
      (let [client (rest/client base-url)
            overview (rest/cluster-overview client)]
        client)
      (catch Exception e
        (println (format "Error: Cannot connect to Flink cluster at %s:%d" host port))
        (println (format "       %s" (.getMessage e)))
        (System/exit 1)))))

(defn- list-jobs
  "List all jobs in the cluster."
  [{:keys [host port status format]}]
  (let [client (connect-to-cluster host port)
        all-jobs (:jobs (rest/list-jobs client))
        jobs (if (or (nil? status) (= (str/lower-case status) "all"))
               all-jobs
               (filter #(= (str/lower-case (:status %)) (str/lower-case status))
                       all-jobs))]
    (if (= format "json")
      (println (clojure.data.json/write-str jobs))
      (if (empty? jobs)
        (println "No jobs found.")
        (do
          (println (format "Jobs on %s:%d" host port))
          (println)
          (print-table
            ["Job ID" "Name" "Status" "Start Time" "Duration"]
            (map (fn [job]
                   [(truncate (:id job) 12)
                    (truncate (or (:name job) "-") 30)
                    (:status job)
                    (format-timestamp (:start-time job))
                    (format-duration (:duration job))])
                 jobs)))))))

(defn- job-info
  "Get detailed job information."
  [{:keys [host port format]} job-id]
  (when (str/blank? job-id)
    (println "Error: Job ID required")
    (println "       Usage: flink-clj jobs info <job-id>")
    (System/exit 1))

  (let [client (connect-to-cluster host port)]
    (try
      (let [details (rest/job-details client job-id)]
        (if (= format "json")
          (println (clojure.data.json/write-str details))
          (do
            (println (format "Job: %s" (:jid details)))
            (println)
            (println (format "  Name:       %s" (:name details)))
            (println (format "  State:      %s" (:state details)))
            (println (format "  Start Time: %s" (format-timestamp (:start-time details))))
            (println (format "  End Time:   %s" (format-timestamp (:end-time details))))
            (println (format "  Duration:   %s" (format-duration (:duration details))))
            (println)
            (when-let [vertices (:vertices details)]
              (println "Vertices:")
              (doseq [v vertices]
                (println (format "  - %s: %s (parallelism: %d)"
                                 (:name v) (:status v) (:parallelism v))))))))
      (catch Exception e
        (println (format "Error: Job not found: %s" job-id))
        (System/exit 1)))))

(defn- cancel-job
  "Cancel a running job."
  [{:keys [host port]} job-id]
  (when (str/blank? job-id)
    (println "Error: Job ID required")
    (println "       Usage: flink-clj jobs cancel <job-id>")
    (System/exit 1))

  (let [client (connect-to-cluster host port)]
    (try
      ;; Verify job exists and is running
      (let [details (rest/job-details client job-id)]
        (when-not (= (:state details) "RUNNING")
          (println (format "Warning: Job is in state '%s', not RUNNING" (:state details)))))

      (rest/cancel-job! client job-id)
      (println (format "Cancellation requested for job: %s" job-id))
      (println)
      (println "Monitor with:")
      (println (format "  flink-clj jobs info %s" job-id))
      (catch Exception e
        (println (format "Error canceling job: %s" (.getMessage e)))
        (System/exit 1)))))

;; =============================================================================
;; Command Entry Point
;; =============================================================================

(defn run
  "Execute the 'jobs' command."
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options :in-order true)
        [subcommand & sub-args] arguments]
    (cond
      (:help options)
      (print-help summary)

      errors
      (do
        (doseq [e errors]
          (println (format "Error: %s" e)))
        (System/exit 1))

      :else
      (case subcommand
        nil (list-jobs options)
        "list" (list-jobs options)
        "info" (job-info options (first sub-args))
        "cancel" (cancel-job options (first sub-args))
        (do
          (println (format "Unknown subcommand: %s" subcommand))
          (println)
          (print-help summary)
          (System/exit 1))))))
