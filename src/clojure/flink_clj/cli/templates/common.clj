(ns flink-clj.cli.templates.common
  "Common template generation utilities."
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io File]))

;; =============================================================================
;; Template Rendering
;; =============================================================================

(defn render
  "Render a template string with variable substitution.

  Variables are in {{variable}} format.

  Example:
    (render \"Hello {{name}}!\" {:name \"World\"})
    ;=> \"Hello World!\""
  [template vars]
  (reduce-kv
    (fn [s k v]
      (str/replace s (str "{{" (name k) "}}") (str v)))
    template
    vars))

(defn write-file!
  "Write content to a file, creating parent directories as needed."
  [^File file content]
  (io/make-parents file)
  (spit file content))

;; =============================================================================
;; Project.clj Template
;; =============================================================================

(defn project-clj-template
  "Generate project.clj content."
  [{:keys [name namespace flink-version template]}]
  (let [flink-dep-version (if (= flink-version "2.x") "2.1.0" "1.20.0")
        kafka-version (if (= flink-version "2.x") "3.4.0-2.1" "3.3.0-1.20")]
    (format "(defproject %s \"0.1.0-SNAPSHOT\"
  :description \"%s Flink job\"
  :license {:name \"MIT\"
            :url \"https://opensource.org/licenses/MIT\"}

  :dependencies [[org.clojure/clojure \"1.11.1\"]
                 [io.github.ugbot/flink-clj \"0.1.0-SNAPSHOT\"]
                 [com.taoensso/nippy \"3.3.0\"]]

  :source-paths [\"src\"]
  :java-source-paths [\"src-java\"]
  :resource-paths [\"resources\"]
  :test-paths [\"test\"]

  :profiles
  {:dev {:dependencies [[org.clojure/tools.namespace \"1.4.4\"]]
         :source-paths [\"dev\"]
         :repl-options {:init-ns user}}

   :provided {:dependencies
              [[org.apache.flink/flink-streaming-java \"%s\"]
               [org.apache.flink/flink-clients \"%s\"]
               [org.apache.flink/flink-connector-kafka \"%s\"]
               [org.apache.flink/flink-connector-base \"%s\"]]}

   :uberjar {:aot :all
             :omit-source true}}

  :main %s.job

  :aliases {\"repl\" [\"with-profile\" \"+dev,+provided\" \"repl\"]
            \"run\" [\"with-profile\" \"+provided\" \"run\"]
            \"uberjar\" [\"with-profile\" \"+provided\" \"uberjar\"]})"
            name
            (str/capitalize (str/replace (str template) "-" " "))
            flink-dep-version
            flink-dep-version
            kafka-version
            flink-dep-version
            namespace)))

;; =============================================================================
;; deps.edn Template
;; =============================================================================

(defn deps-edn-template
  "Generate deps.edn content."
  [{:keys [name namespace flink-version template]}]
  (let [flink-dep-version (if (= flink-version "2.x") "2.1.0" "1.20.0")
        kafka-version (if (= flink-version "2.x") "3.4.0-2.1" "3.3.0-1.20")]
    (format "{:paths [\"src\" \"resources\"]

 :deps {org.clojure/clojure {:mvn/version \"1.11.1\"}
        io.github.ugbot/flink-clj {:mvn/version \"0.1.0-SNAPSHOT\"}
        com.taoensso/nippy {:mvn/version \"3.3.0\"}}

 :aliases
 {:dev {:extra-paths [\"dev\" \"test\"]
        :extra-deps {org.clojure/tools.namespace {:mvn/version \"1.4.4\"}}}

  :flink {:extra-deps
          {org.apache.flink/flink-streaming-java {:mvn/version \"%s\"}
           org.apache.flink/flink-clients {:mvn/version \"%s\"}
           org.apache.flink/flink-connector-kafka {:mvn/version \"%s\"}
           org.apache.flink/flink-connector-base {:mvn/version \"%s\"}}}

  :run {:main-opts [\"-m\" \"%s.job\"]
        :extra-deps {org.apache.flink/flink-streaming-java {:mvn/version \"%s\"}
                     org.apache.flink/flink-clients {:mvn/version \"%s\"}}}

  :uberjar {:replace-deps {com.github.seancorfield/depstar {:mvn/version \"2.1.303\"}}
            :exec-fn hf.depstar/uberjar
            :exec-args {:jar \"target/%s.jar\"
                        :aot true
                        :main-class \"%s.job\"}}}}"
            flink-dep-version
            flink-dep-version
            kafka-version
            flink-dep-version
            namespace
            flink-dep-version
            flink-dep-version
            name
            namespace)))

;; =============================================================================
;; User.clj (REPL) Template
;; =============================================================================

(defn user-clj-template
  "Generate dev/user.clj content."
  [{:keys [namespace]}]
  (format "(ns user
  \"Development namespace for REPL-driven development.

  Quick start:
    (start!)                  ; Initialize local Flink
    (start! {:web-ui true})   ; With Web UI at localhost:8081
    env                       ; StreamExecutionEnvironment
    table-env                 ; StreamTableEnvironment
    (stop!)                   ; Cleanup\"
  (:require [clojure.tools.namespace.repl :refer [refresh]]
            [flink-clj.shell :as shell]
            [flink-clj.dsl :as f]
            [%s.job :as job]))

;; Re-export shell functions
(def start! shell/start!)
(def stop! shell/stop!)
(def restart! shell/restart!)
(def collect shell/collect)
(def take-n shell/take-n)
(def desc shell/desc)
(def explain shell/explain)
(def help shell/help)

;; Convenient environment bindings (available after start!)
(def env shell/env)
(def table-env shell/table-env)

;; Development utilities
(defn reset []
  \"Reload all changed namespaces.\"
  (refresh))

(println)
(println \"=== %s REPL ===\")
(println \"(start!)              - Initialize local Flink\")
(println \"(start! {:web-ui true}) - With Web UI\")
(println \"(help)                - Show all commands\")
(println)"
          namespace
          namespace))

;; =============================================================================
;; .gitignore Template
;; =============================================================================

(def gitignore-template
  "# Clojure/Leiningen
target/
classes/
.lein-*
.nrepl-port
*.jar
*.class
pom.xml
pom.xml.asc

# deps.edn
.cpcache/
.clj-kondo/

# IDE
.idea/
*.iml
.vscode/
.calva/
*.swp
*~

# OS
.DS_Store
Thumbs.db

# Flink
flink-savepoints/
flink-checkpoints/
*.log

# Environment
.env
.env.local
")

;; =============================================================================
;; README Template
;; =============================================================================

(defn readme-template
  "Generate README.md content."
  [{:keys [name template]}]
  (let [template-name (str/capitalize (str/replace (str template) "-" " "))]
    (format "# %s

%s Flink job built with flink-clj.

## Development

Start a REPL:

```bash
# With Leiningen
lein repl

# Or with flink-clj CLI
flink-clj repl --web-ui
```

In the REPL:

```clojure
(start!)                    ; Initialize Flink environment
(start! {:web-ui true})     ; With Web UI at localhost:8081

;; Test your transformations
(require '[flink-clj.dsl :as f])
(require '[%s.job :as job])

;; Create a test pipeline
(-> (f/source env (f/collection [1 2 3 4 5]))
    (f/map inc)
    (collect))
;=> [2 3 4 5 6]

(stop!)                     ; Cleanup
```

## Build

```bash
# Build uberjar
lein uberjar
# or
flink-clj build

# Output: target/%s-0.1.0-SNAPSHOT-standalone.jar
```

## Deploy

```bash
# Local
flink-clj deploy target/%s.jar --target local

# Standalone cluster
flink-clj deploy target/%s.jar --target standalone --host flink-cluster

# Kubernetes
flink-clj deploy target/%s.jar --target k8s --name %s
```

## License

MIT
"
            name
            template-name
            (str/replace name "-" "_")
            name
            name
            name
            name
            name)))

;; =============================================================================
;; Template Dispatch
;; =============================================================================

(defmulti job-template
  "Generate job.clj content based on template type."
  (fn [opts] (:template opts)))

;; =============================================================================
;; ETL Template
;; =============================================================================

(defmethod job-template :etl
  [{:keys [name namespace]}]
  (format "(ns %s.job
  \"ETL Pipeline: Extract, Transform, Load

  A streaming ETL job that reads from a source, applies transformations,
  and writes to a sink.

  Usage:
    lein run
    # or
    flink-clj run\"
  (:require [flink-clj.dsl :as f]
            [flink-clj.env :as env]
            [clojure.string :as str])
  (:gen-class))

;; =============================================================================
;; Transformations
;; =============================================================================

(defn parse-record
  \"Parse raw input string into a structured record.\"
  [raw]
  (try
    (read-string raw)
    (catch Exception _
      {:error true :raw raw})))

(defn valid?
  \"Filter invalid records.\"
  [record]
  (and (map? record)
       (not (:error record))
       (:id record)))

(defn transform
  \"Apply business transformations.\"
  [record]
  (-> record
      (update :timestamp #(or %% (System/currentTimeMillis)))
      (update :processed (constantly true))))

(defn format-output
  \"Format record for output.\"
  [record]
  (pr-str record))

;; =============================================================================
;; Configuration
;; =============================================================================

(def config
  {:kafka {:bootstrap-servers (or (System/getenv \"KAFKA_SERVERS\") \"localhost:9092\")
           :input-topic (or (System/getenv \"INPUT_TOPIC\") \"input\")
           :output-topic (or (System/getenv \"OUTPUT_TOPIC\") \"output\")
           :group-id \"%s-etl\"}
   :flink {:parallelism (Integer/parseInt (or (System/getenv \"PARALLELISM\") \"4\"))
           :checkpoint-interval 60000}})

;; =============================================================================
;; Pipeline
;; =============================================================================

(defn build-pipeline
  \"Build the ETL pipeline.\"
  [env config]
  (let [{:keys [kafka]} config]
    (-> (f/source env (f/kafka {:servers (:bootstrap-servers kafka)
                                 :topic (:input-topic kafka)
                                 :group-id (:group-id kafka)}))
        (f/map parse-record)
        (f/filter valid?)
        (f/map transform)
        (f/map format-output)
        (f/sink (f/kafka {:servers (:bootstrap-servers kafka)
                          :topic (:output-topic kafka)})))))

;; =============================================================================
;; Main Entry Point
;; =============================================================================

(defn -main
  \"Main entry point for the ETL job.\"
  [& args]
  (let [env (env/create-env {:parallelism (get-in config [:flink :parallelism])
                              :checkpoint {:interval (get-in config [:flink :checkpoint-interval])
                                           :mode :exactly-once}})]
    (println \"Starting %s ETL Pipeline...\")
    (build-pipeline env config)
    (f/run env \"%s ETL Pipeline\")))"
          namespace
          name
          name
          name))

;; =============================================================================
;; Analytics Template
;; =============================================================================

(defmethod job-template :analytics
  [{:keys [name namespace]}]
  (format "(ns %s.job
  \"Stream Analytics: Real-time Aggregations

  A streaming analytics job that computes real-time metrics and aggregations.

  Usage:
    lein run
    # or
    flink-clj run\"
  (:require [flink-clj.dsl :as f]
            [flink-clj.env :as env]
            [clojure.string :as str])
  (:gen-class))

;; =============================================================================
;; Event Processing
;; =============================================================================

(defn parse-event
  \"Parse raw event string.\"
  [raw]
  (try
    (read-string raw)
    (catch Exception _
      nil)))

(defn extract-key
  \"Extract key for grouping events.\"
  [event]
  (:user-id event))

(defn valid-event?
  \"Filter valid events.\"
  [event]
  (and (map? event)
       (:user-id event)
       (:value event)))

;; =============================================================================
;; Aggregation
;; =============================================================================

(defn create-aggregate
  \"Create initial aggregate state.\"
  []
  {:count 0
   :total 0.0
   :min Double/MAX_VALUE
   :max Double/MIN_VALUE})

(defn aggregate
  \"Aggregate function for window.\"
  [acc event]
  (let [value (double (:value event))]
    (-> acc
        (update :count inc)
        (update :total + value)
        (update :min min value)
        (update :max max value))))

(defn merge-aggregates
  \"Merge two aggregates (for parallel processing).\"
  [a b]
  {:count (+ (:count a) (:count b))
   :total (+ (:total a) (:total b))
   :min (min (:min a) (:min b))
   :max (max (:max a) (:max b))})

(defn finalize-aggregate
  \"Compute final metrics from aggregate.\"
  [agg key window-end]
  (let [{:keys [count total min max]} agg]
    {:user-id key
     :window-end window-end
     :count count
     :total total
     :average (if (pos? count) (/ total count) 0.0)
     :min (if (= min Double/MAX_VALUE) 0.0 min)
     :max (if (= max Double/MIN_VALUE) 0.0 max)}))

;; =============================================================================
;; Configuration
;; =============================================================================

(def config
  {:kafka {:bootstrap-servers (or (System/getenv \"KAFKA_SERVERS\") \"localhost:9092\")
           :input-topic (or (System/getenv \"INPUT_TOPIC\") \"events\")
           :output-topic (or (System/getenv \"OUTPUT_TOPIC\") \"aggregates\")
           :group-id \"%s-analytics\"}
   :flink {:parallelism (Integer/parseInt (or (System/getenv \"PARALLELISM\") \"4\"))
           :window-size-minutes 1}})

;; =============================================================================
;; Pipeline
;; =============================================================================

(defn build-pipeline
  \"Build the analytics pipeline.\"
  [env config]
  (let [{:keys [kafka flink]} config]
    (-> (f/source env (f/kafka {:servers (:bootstrap-servers kafka)
                                 :topic (:input-topic kafka)
                                 :group-id (:group-id kafka)}))
        (f/map parse-event)
        (f/filter valid-event?)
        (f/key-by extract-key)
        (f/tumbling (:window-size-minutes flink) :minutes)
        (f/reduce aggregate (create-aggregate))
        (f/map pr-str)
        (f/sink (f/kafka {:servers (:bootstrap-servers kafka)
                          :topic (:output-topic kafka)})))))

;; =============================================================================
;; Main Entry Point
;; =============================================================================

(defn -main
  \"Main entry point for the analytics job.\"
  [& args]
  (let [env (env/create-env {:parallelism (get-in config [:flink :parallelism])})]
    (println \"Starting %s Analytics Pipeline...\")
    (build-pipeline env config)
    (f/run env \"%s Analytics\")))"
          namespace
          name
          name
          name))

;; =============================================================================
;; CDC Template
;; =============================================================================

(defmethod job-template :cdc
  [{:keys [name namespace]}]
  (format "(ns %s.job
  \"CDC Processor: Change Data Capture

  A streaming CDC job that captures database changes and processes them.

  Usage:
    lein run
    # or
    flink-clj run\"
  (:require [flink-clj.dsl :as f]
            [flink-clj.env :as env]
            [flink-clj.connectors.cdc :as cdc]
            [clojure.string :as str])
  (:gen-class))

;; =============================================================================
;; Change Processing
;; =============================================================================

(defn classify-change
  \"Classify the type of change.\"
  [change]
  (case (:op change)
    \"c\" :insert
    \"u\" :update
    \"d\" :delete
    \"r\" :read  ; snapshot read
    :unknown))

(defn extract-before-after
  \"Extract before/after values from CDC event.\"
  [change]
  {:op (classify-change change)
   :before (:before change)
   :after (:after change)
   :source (:source change)
   :ts_ms (:ts_ms change)})

(defn process-insert
  \"Handle insert operations.\"
  [change]
  {:type :insert
   :data (:after change)
   :timestamp (:ts_ms change)})

(defn process-update
  \"Handle update operations.\"
  [change]
  {:type :update
   :before (:before change)
   :after (:after change)
   :changed-fields (when (and (:before change) (:after change))
                     (keys (filter (fn [[k v]]
                                     (not= v (get (:before change) k)))
                                   (:after change))))
   :timestamp (:ts_ms change)})

(defn process-delete
  \"Handle delete operations.\"
  [change]
  {:type :delete
   :data (:before change)
   :timestamp (:ts_ms change)})

(defn process-change
  \"Route change to appropriate handler.\"
  [change]
  (let [classified (extract-before-after change)]
    (case (:op classified)
      :insert (process-insert classified)
      :update (process-update classified)
      :delete (process-delete classified)
      :read (process-insert classified)  ; Treat snapshot reads as inserts
      {:type :unknown :raw change})))

;; =============================================================================
;; Configuration
;; =============================================================================

(def config
  {:mysql {:hostname (or (System/getenv \"MYSQL_HOST\") \"localhost\")
           :port (Integer/parseInt (or (System/getenv \"MYSQL_PORT\") \"3306\"))
           :username (or (System/getenv \"MYSQL_USER\") \"root\")
           :password (or (System/getenv \"MYSQL_PASSWORD\") \"password\")
           :database (or (System/getenv \"MYSQL_DATABASE\") \"mydb\")
           :table (or (System/getenv \"MYSQL_TABLE\") \"orders\")}
   :kafka {:bootstrap-servers (or (System/getenv \"KAFKA_SERVERS\") \"localhost:9092\")
           :output-topic (or (System/getenv \"OUTPUT_TOPIC\") \"cdc-events\")}
   :flink {:parallelism (Integer/parseInt (or (System/getenv \"PARALLELISM\") \"2\"))
           :checkpoint-interval 30000}})

;; =============================================================================
;; Pipeline
;; =============================================================================

(defn build-pipeline
  \"Build the CDC pipeline.\"
  [env config]
  (let [{:keys [mysql kafka]} config]
    (-> (f/source env (cdc/mysql {:hostname (:hostname mysql)
                                   :port (:port mysql)
                                   :username (:username mysql)
                                   :password (:password mysql)
                                   :database-list [(:database mysql)]
                                   :table-list [(str (:database mysql) \".\" (:table mysql))]}))
        (f/map process-change)
        (f/map pr-str)
        (f/sink (f/kafka {:servers (:bootstrap-servers kafka)
                          :topic (:output-topic kafka)})))))

;; =============================================================================
;; Main Entry Point
;; =============================================================================

(defn -main
  \"Main entry point for the CDC job.\"
  [& args]
  (let [env (env/create-env {:parallelism (get-in config [:flink :parallelism])
                              :checkpoint {:interval (get-in config [:flink :checkpoint-interval])
                                           :mode :exactly-once}})]
    (println \"Starting %s CDC Processor...\")
    (println (format \"Capturing changes from %%s.%%s\"
                     (get-in config [:mysql :database])
                     (get-in config [:mysql :table])))
    (build-pipeline env config)
    (f/run env \"%s CDC\")))"
          namespace
          name
          name))

;; =============================================================================
;; Event-Driven Template
;; =============================================================================

(defmethod job-template :event-driven
  [{:keys [name namespace]}]
  (format "(ns %s.job
  \"Event-Driven: Pattern Matching and CEP

  A streaming job that detects complex event patterns.

  Usage:
    lein run
    # or
    flink-clj run\"
  (:require [flink-clj.dsl :as f]
            [flink-clj.env :as env]
            [flink-clj.cep :as cep]
            [clojure.string :as str])
  (:gen-class))

;; =============================================================================
;; Event Processing
;; =============================================================================

(defn parse-event
  \"Parse raw event string.\"
  [raw]
  (try
    (read-string raw)
    (catch Exception _
      nil)))

(defn valid-event?
  \"Filter valid events.\"
  [event]
  (and (map? event)
       (:user-id event)
       (:type event)))

(defn extract-key
  \"Extract key for pattern detection.\"
  [event]
  (:user-id event))

;; =============================================================================
;; Pattern Definitions
;; =============================================================================

;; Pattern: Detect rapid succession of events (potential fraud)
(defn small-transaction?
  \"Check if transaction is small.\"
  [event]
  (and (= (:type event) :transaction)
       (< (:amount event 0) 100)))

(defn large-transaction?
  \"Check if transaction is large.\"
  [event]
  (and (= (:type event) :transaction)
       (> (:amount event 0) 10000)))

(def fraud-pattern
  \"Detect small transaction followed by large transaction within 10 minutes.\"
  (cep/pattern
    (cep/begin :small-tx small-transaction?)
    (cep/followed-by :large-tx large-transaction?)
    (cep/within 10 :minutes)))

;; Pattern: Detect login anomalies
(defn login-event?
  \"Check if event is a login.\"
  [event]
  (= (:type event) :login))

(defn failed-login?
  \"Check if login failed.\"
  [event]
  (and (login-event? event)
       (= (:status event) :failed)))

(def brute-force-pattern
  \"Detect 3 or more failed logins within 5 minutes.\"
  (cep/pattern
    (cep/begin :first-fail failed-login?)
    (cep/followed-by :second-fail failed-login?)
    (cep/followed-by :third-fail failed-login?)
    (cep/within 5 :minutes)))

;; =============================================================================
;; Alert Generation
;; =============================================================================

(defn create-fraud-alert
  \"Generate fraud alert from matched pattern.\"
  [matched]
  {:alert-type :potential-fraud
   :user-id (:user-id (first (:small-tx matched)))
   :small-tx (first (:small-tx matched))
   :large-tx (first (:large-tx matched))
   :timestamp (System/currentTimeMillis)
   :severity :high})

(defn create-brute-force-alert
  \"Generate brute force alert from matched pattern.\"
  [matched]
  {:alert-type :brute-force-attempt
   :user-id (:user-id (first (:first-fail matched)))
   :failed-attempts (concat (:first-fail matched)
                           (:second-fail matched)
                           (:third-fail matched))
   :timestamp (System/currentTimeMillis)
   :severity :critical})

;; =============================================================================
;; Configuration
;; =============================================================================

(def config
  {:kafka {:bootstrap-servers (or (System/getenv \"KAFKA_SERVERS\") \"localhost:9092\")
           :input-topic (or (System/getenv \"INPUT_TOPIC\") \"events\")
           :alerts-topic (or (System/getenv \"ALERTS_TOPIC\") \"alerts\")
           :group-id \"%s-cep\"}
   :flink {:parallelism (Integer/parseInt (or (System/getenv \"PARALLELISM\") \"4\"))}})

;; =============================================================================
;; Pipeline
;; =============================================================================

(defn build-pipeline
  \"Build the event-driven pipeline.\"
  [env config]
  (let [{:keys [kafka]} config
        events (-> (f/source env (f/kafka {:servers (:bootstrap-servers kafka)
                                            :topic (:input-topic kafka)
                                            :group-id (:group-id kafka)}))
                   (f/map parse-event)
                   (f/filter valid-event?)
                   (f/key-by extract-key))]

    ;; Detect fraud patterns
    (-> events
        (cep/detect fraud-pattern)
        (f/map create-fraud-alert)
        (f/map pr-str)
        (f/sink (f/kafka {:servers (:bootstrap-servers kafka)
                          :topic (:alerts-topic kafka)})))

    ;; Detect brute force patterns
    (-> events
        (cep/detect brute-force-pattern)
        (f/map create-brute-force-alert)
        (f/map pr-str)
        (f/sink (f/kafka {:servers (:bootstrap-servers kafka)
                          :topic (:alerts-topic kafka)})))))

;; =============================================================================
;; Main Entry Point
;; =============================================================================

(defn -main
  \"Main entry point for the event-driven job.\"
  [& args]
  (let [env (env/create-env {:parallelism (get-in config [:flink :parallelism])})]
    (println \"Starting %s Event Detection Pipeline...\")
    (build-pipeline env config)
    (f/run env \"%s Event Detection\")))"
          namespace
          name
          name
          name))

;; =============================================================================
;; Project Generation
;; =============================================================================

(defn generate-project!
  "Generate a complete project from template."
  [^File project-dir {:keys [name namespace template flink-version deps-edn?] :as opts}]
  (let [src-dir (io/file project-dir "src" namespace)
        dev-dir (io/file project-dir "dev")]

    ;; project.clj or deps.edn
    (if deps-edn?
      (write-file! (io/file project-dir "deps.edn")
                   (deps-edn-template opts))
      (write-file! (io/file project-dir "project.clj")
                   (project-clj-template opts)))

    ;; Main job file
    (write-file! (io/file src-dir "job.clj")
                 (job-template opts))

    ;; dev/user.clj
    (write-file! (io/file dev-dir "user.clj")
                 (user-clj-template opts))

    ;; .gitignore
    (write-file! (io/file project-dir ".gitignore")
                 gitignore-template)

    ;; README.md
    (write-file! (io/file project-dir "README.md")
                 (readme-template opts))

    project-dir))
