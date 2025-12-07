(ns flink-clj.ml
  "Machine Learning model inference support for Flink SQL.

  This namespace provides Clojure wrappers for Flink's ML_PREDICT functionality
  introduced in FLIP-437. It allows registering ML models and performing
  inference directly in Flink SQL pipelines.

  Supported providers:
  - OpenAI (chat completions, embeddings)
  - Azure OpenAI
  - Custom OpenAI-compatible endpoints

  Basic usage:
    (require '[flink-clj.ml :as ml])
    (require '[flink-clj.table :as t])

    ;; Create a table environment with ML support
    (def table-env (t/create-table-env stream-env))

    ;; Register an OpenAI model
    (ml/register-model! table-env \"chat_model\"
      (ml/openai-model {:api-key \"sk-xxx\"
                        :model \"gpt-4o-mini\"
                        :task :chat}))

    ;; Use in SQL queries
    (t/execute-sql table-env
      \"SELECT ML_PREDICT('chat_model', prompt) AS response FROM prompts\")

  See also:
  - FLIP-437: https://cwiki.apache.org/confluence/display/FLINK/FLIP-437
  - Flink ML Docs: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/ml/"
  (:require [clojure.string :as str]
            [flink-clj.version :as v]
            [flink-clj.table :as t]))

;; =============================================================================
;; Version and Availability Checks
;; =============================================================================

(defn- ml-api-available?
  "Check if ML_PREDICT functionality is available (Flink 2.1+)."
  []
  (and (v/flink-2?)
       (t/table-api-available?)))

(defn- ensure-ml-available!
  "Throw if ML API is not available."
  [feature-name]
  (when-not (ml-api-available?)
    (throw (ex-info (str feature-name " requires Flink ML support (Flink 2.1+ with Table API)")
                    {:flink-version (v/flink-minor-version)
                     :hint "Ensure flink-table-api-java-bridge and ML dependencies are available"}))))

(defn ml-available?
  "Check if Flink ML_PREDICT functionality is available.

  Returns true if running on Flink 2.1+ with Table API support."
  []
  (ml-api-available?))

(defn ml-info
  "Get information about ML API availability.

  Returns a map with:
    :available - Boolean indicating if ML is available
    :version - Flink version string
    :providers - List of supported providers"
  []
  {:available (ml-available?)
   :version (v/flink-minor-version)
   :providers (if (ml-available?)
                [:openai :azure-openai :custom]
                [])
   :features {:chat-completions true
              :embeddings true
              :async-inference true
              :batching true}})

;; =============================================================================
;; Table Environment (delegates to flink-clj.table)
;; =============================================================================

(defn create-table-env
  "Create a StreamTableEnvironment for ML operations.

  DEPRECATED: Use flink-clj.table/create-table-env instead.

  Arguments:
    stream-env - A StreamExecutionEnvironment

  Example:
    (def stream-env (env/create-env))
    (def table-env (t/create-table-env stream-env))"
  [stream-env]
  (ensure-ml-available! "create-table-env")
  (t/create-table-env stream-env))

(defn execute-sql
  "Execute a SQL statement in the table environment.

  DEPRECATED: Use flink-clj.table/execute-sql instead.

  Returns a TableResult.

  Example:
    (t/execute-sql table-env \"SELECT * FROM my_table\")"
  [table-env sql]
  (ensure-ml-available! "execute-sql")
  (t/execute-sql table-env sql))

;; =============================================================================
;; Model Configuration Builders
;; =============================================================================

(defn openai-model
  "Create an OpenAI model configuration.

  Options:
    :api-key     - OpenAI API key (required)
    :model       - Model name (e.g., \"gpt-4o-mini\", \"text-embedding-3-small\")
    :task        - :chat or :embedding (default: :chat)
    :endpoint    - Custom endpoint URL (optional)
    :system-prompt - System prompt for chat models (optional)
    :temperature - Sampling temperature 0-2 (optional)
    :max-tokens  - Maximum tokens to generate (optional)
    :timeout     - Request timeout in seconds (optional, default: 60)

  Async/Batching options:
    :async?           - Enable async inference (default: true)
    :max-batch-size   - Maximum batch size (default: 16)
    :batch-timeout-ms - Batch timeout in milliseconds (default: 1000)
    :max-retries      - Maximum retry attempts (default: 3)
    :rate-limit-rpm   - Rate limit requests per minute (optional)

  Example:
    (openai-model {:api-key \"sk-xxx\"
                   :model \"gpt-4o-mini\"
                   :task :chat
                   :system-prompt \"You are a helpful assistant.\"
                   :temperature 0.7
                   :max-tokens 100})"
  [{:keys [api-key model task endpoint system-prompt temperature max-tokens timeout
           async? max-batch-size batch-timeout-ms max-retries rate-limit-rpm]
    :or {task :chat
         async? true
         max-batch-size 16
         batch-timeout-ms 1000
         max-retries 3
         timeout 60}}]
  (when-not api-key
    (throw (ex-info "OpenAI model requires :api-key" {})))
  (when-not model
    (throw (ex-info "OpenAI model requires :model name" {})))
  {:provider :openai
   :config (cond-> {:api-key api-key
                    :model model
                    :task (name task)
                    :async async?
                    :max-batch-size max-batch-size
                    :batch-timeout-ms batch-timeout-ms
                    :max-retries max-retries
                    :timeout timeout}
             endpoint (assoc :endpoint endpoint)
             system-prompt (assoc :system-prompt system-prompt)
             temperature (assoc :temperature temperature)
             max-tokens (assoc :max-tokens max-tokens)
             rate-limit-rpm (assoc :rate-limit-rpm rate-limit-rpm))})

(defn azure-openai-model
  "Create an Azure OpenAI model configuration.

  Options:
    :api-key       - Azure OpenAI API key (required)
    :endpoint      - Azure OpenAI endpoint URL (required)
    :deployment    - Deployment name (required)
    :api-version   - API version (default: \"2024-02-15-preview\")
    :task          - :chat or :embedding (default: :chat)

  Plus all options from openai-model for behavior configuration.

  Example:
    (azure-openai-model {:api-key \"xxx\"
                         :endpoint \"https://my-resource.openai.azure.com\"
                         :deployment \"gpt-4o\"
                         :task :chat})"
  [{:keys [api-key endpoint deployment api-version task]
    :or {api-version "2024-02-15-preview"
         task :chat}
    :as opts}]
  (when-not api-key
    (throw (ex-info "Azure OpenAI model requires :api-key" {})))
  (when-not endpoint
    (throw (ex-info "Azure OpenAI model requires :endpoint" {})))
  (when-not deployment
    (throw (ex-info "Azure OpenAI model requires :deployment" {})))
  (-> (openai-model (assoc opts :model deployment))
      (assoc :provider :azure-openai)
      (assoc-in [:config :endpoint] endpoint)
      (assoc-in [:config :api-version] api-version)))

(defn custom-model
  "Create a custom OpenAI-compatible model configuration.

  For use with local models (Ollama, vLLM, etc.) or other compatible APIs.

  Options:
    :endpoint    - API endpoint URL (required)
    :model       - Model name (required)
    :task        - :chat or :embedding (default: :chat)
    :api-key     - API key (optional, some endpoints don't require it)

  Plus all options from openai-model for behavior configuration.

  Example:
    (custom-model {:endpoint \"http://localhost:11434/v1\"
                   :model \"llama3.2\"
                   :task :chat})"
  [{:keys [endpoint model task api-key]
    :or {task :chat
         api-key ""}
    :as opts}]
  (when-not endpoint
    (throw (ex-info "Custom model requires :endpoint" {})))
  (when-not model
    (throw (ex-info "Custom model requires :model name" {})))
  (-> (openai-model (assoc opts :api-key (or api-key "")))
      (assoc :provider :custom)
      (assoc-in [:config :endpoint] endpoint)))

;; =============================================================================
;; CREATE MODEL SQL Generation
;; =============================================================================

(defn- escape-sql-string
  "Escape single quotes in SQL strings."
  [s]
  (when s
    (str/replace (str s) "'" "''")))

(defn- config->sql-options
  "Convert config map to SQL WITH clause options."
  [config]
  (let [opt-str (fn [k v]
                  (str "'" (name k) "' = '" (escape-sql-string (str v)) "'"))]
    (->> config
         (map (fn [[k v]] (opt-str k v)))
         (str/join ",\n    "))))

(defn- model->create-sql
  "Generate CREATE MODEL SQL from model configuration."
  [model-name {:keys [provider config]}]
  (let [provider-name (case provider
                        :openai "openai"
                        :azure-openai "azure-openai"
                        :custom "openai"  ; Custom uses OpenAI-compatible protocol
                        (str (name provider)))
        sql-config (-> config
                       (assoc :provider provider-name)
                       config->sql-options)]
    (format "CREATE MODEL `%s`
WITH (
    %s
)" model-name sql-config)))

(defn register-model!
  "Register an ML model in the table environment.

  Arguments:
    table-env  - A StreamTableEnvironment
    model-name - Name for the model (used in ML_PREDICT)
    model-config - Model configuration from openai-model, azure-openai-model, or custom-model

  Example:
    (ml/register-model! table-env \"chat_model\"
      (ml/openai-model {:api-key \"sk-xxx\"
                        :model \"gpt-4o-mini\"}))"
  [table-env model-name model-config]
  (ensure-ml-available! "register-model!")
  (let [sql (model->create-sql model-name model-config)]
    (t/execute-sql table-env sql)))

(defn drop-model!
  "Drop an ML model from the table environment.

  Arguments:
    table-env  - A StreamTableEnvironment
    model-name - Name of the model to drop
    if-exists? - If true, don't error if model doesn't exist (default: true)

  Example:
    (ml/drop-model! table-env \"old_model\")"
  ([table-env model-name]
   (drop-model! table-env model-name true))
  ([table-env model-name if-exists?]
   (ensure-ml-available! "drop-model!")
   (let [sql (if if-exists?
               (format "DROP MODEL IF EXISTS `%s`" model-name)
               (format "DROP MODEL `%s`" model-name))]
     (t/execute-sql table-env sql))))

;; =============================================================================
;; ML_PREDICT Helpers
;; =============================================================================

(defn predict-sql
  "Generate an ML_PREDICT SQL expression.

  Arguments:
    model-name - Name of the registered model
    input-expr - SQL expression for input (column name or expression)

  Options:
    :args      - Additional arguments as a map
    :output-as - Alias for the output column

  Example:
    (predict-sql \"chat_model\" \"prompt\")
    ;=> \"ML_PREDICT('chat_model', prompt)\"

    (predict-sql \"chat_model\" \"prompt\" {:output-as \"response\"})
    ;=> \"ML_PREDICT('chat_model', prompt) AS response\""
  ([model-name input-expr]
   (predict-sql model-name input-expr {}))
  ([model-name input-expr {:keys [args output-as]}]
   (let [base (if args
                (format "ML_PREDICT('%s', %s, %s)"
                        model-name input-expr
                        (format "MAP[%s]"
                                (->> args
                                     (map (fn [[k v]]
                                            (format "'%s', '%s'" (name k) (escape-sql-string (str v)))))
                                     (str/join ", "))))
                (format "ML_PREDICT('%s', %s)" model-name input-expr))]
     (if output-as
       (str base " AS " output-as)
       base))))

(defn predict-query
  "Generate a complete SELECT query with ML_PREDICT.

  Arguments:
    model-name  - Name of the registered model
    input-col   - Input column name
    source-table - Source table or subquery
    output-col  - Output column alias (default: \"prediction\")

  Example:
    (predict-query \"chat_model\" \"prompt\" \"messages\" \"response\")
    ;=> \"SELECT ML_PREDICT('chat_model', prompt) AS response FROM messages\""
  ([model-name input-col source-table]
   (predict-query model-name input-col source-table "prediction"))
  ([model-name input-col source-table output-col]
   (format "SELECT %s FROM %s"
           (predict-sql model-name input-col {:output-as output-col})
           source-table)))

;; =============================================================================
;; Streaming Inference Pipeline (delegates to flink-clj.table)
;; =============================================================================

(defn stream-to-table
  "Convert a DataStream to a Table for ML processing.

  DEPRECATED: Use flink-clj.table/from-data-stream instead.

  Arguments:
    table-env - A StreamTableEnvironment
    stream    - A DataStream
    schema    - Column names (vector of keywords or strings)

  Example:
    (t/from-data-stream table-env data-stream)"
  [table-env stream schema]
  (ensure-ml-available! "stream-to-table")
  (t/from-data-stream table-env stream {:schema schema}))

(defn table-to-stream
  "Convert a Table back to a DataStream.

  DEPRECATED: Use flink-clj.table/to-data-stream instead.

  Arguments:
    table-env - A StreamTableEnvironment
    table     - A Table
    row-class - Optional: Java class for the Row type (default: Row)

  Example:
    (t/to-data-stream table table-env)"
  ([table-env table]
   (table-to-stream table-env table nil))
  ([table-env table row-class]
   (ensure-ml-available! "table-to-stream")
   (t/to-data-stream table table-env {:class row-class})))

;; =============================================================================
;; Convenience Macros
;; =============================================================================

(defmacro with-model
  "Execute body with a registered ML model, automatically cleaning up.

  Example:
    (ml/with-model [model-name table-env (ml/openai-model {...})]
      (ml/execute-sql table-env (ml/predict-query model-name \"prompt\" \"data\")))"
  [[model-sym table-env model-config] & body]
  `(let [~model-sym (str "temp_model_" (System/currentTimeMillis))]
     (try
       (register-model! ~table-env ~model-sym ~model-config)
       ~@body
       (finally
         (drop-model! ~table-env ~model-sym true)))))

;; =============================================================================
;; Embeddings Helpers
;; =============================================================================

(defn embedding-model
  "Create an OpenAI embedding model configuration.

  Shorthand for openai-model with :task :embedding.

  Options:
    :api-key     - OpenAI API key (required)
    :model       - Model name (default: \"text-embedding-3-small\")
    :dimensions  - Output dimensions (optional, model-dependent)

  Example:
    (embedding-model {:api-key \"sk-xxx\"
                      :model \"text-embedding-3-large\"
                      :dimensions 1024})"
  [{:keys [model dimensions] :as opts
    :or {model "text-embedding-3-small"}}]
  (-> (openai-model (assoc opts :model model :task :embedding))
      (cond-> dimensions (assoc-in [:config :dimensions] dimensions))))

;; =============================================================================
;; Chat Helpers
;; =============================================================================

(defn chat-model
  "Create an OpenAI chat model configuration.

  Shorthand for openai-model with :task :chat and common defaults.

  Options:
    :api-key       - OpenAI API key (required)
    :model         - Model name (default: \"gpt-4o-mini\")
    :system-prompt - System message (optional)
    :temperature   - Sampling temperature (default: 0.7)
    :max-tokens    - Maximum output tokens (optional)

  Example:
    (chat-model {:api-key \"sk-xxx\"
                 :system-prompt \"You are a helpful assistant.\"
                 :temperature 0.5})"
  [{:keys [model temperature] :as opts
    :or {model "gpt-4o-mini"
         temperature 0.7}}]
  (openai-model (assoc opts :model model :temperature temperature :task :chat)))

;; =============================================================================
;; Provider Configuration Templates
;; =============================================================================

(defn ollama-model
  "Create a configuration for local Ollama models.

  Options:
    :model    - Model name (e.g., \"llama3.2\", \"mistral\")
    :endpoint - Ollama API endpoint (default: \"http://localhost:11434/v1\")
    :task     - :chat or :embedding (default: :chat)

  Example:
    (ollama-model {:model \"llama3.2\"})"
  [{:keys [model endpoint task]
    :or {endpoint "http://localhost:11434/v1"
         task :chat}
    :as opts}]
  (when-not model
    (throw (ex-info "Ollama model requires :model name" {})))
  (custom-model (assoc opts
                       :endpoint endpoint
                       :task task)))

(defn vllm-model
  "Create a configuration for vLLM server.

  Options:
    :model    - Model name (e.g., \"meta-llama/Llama-3.2-3B-Instruct\")
    :endpoint - vLLM API endpoint (default: \"http://localhost:8000/v1\")
    :task     - :chat or :embedding (default: :chat)

  Example:
    (vllm-model {:model \"meta-llama/Llama-3.2-3B-Instruct\"})"
  [{:keys [model endpoint task]
    :or {endpoint "http://localhost:8000/v1"
         task :chat}
    :as opts}]
  (when-not model
    (throw (ex-info "vLLM model requires :model name" {})))
  (custom-model (assoc opts
                       :endpoint endpoint
                       :task task)))

;; =============================================================================
;; Batch Inference Configuration
;; =============================================================================

(defn batch-config
  "Create batch inference configuration for ML models.

  Options:
    :max-batch-size   - Maximum records per batch (default: 16)
    :batch-timeout-ms - Maximum wait time for batch (default: 1000)
    :parallelism      - Inference parallelism (optional)

  Returns a map that can be merged into model config.

  Example:
    (openai-model (merge
                    {:api-key \"sk-xxx\" :model \"gpt-4o-mini\"}
                    (batch-config {:max-batch-size 32})))"
  [{:keys [max-batch-size batch-timeout-ms parallelism]
    :or {max-batch-size 16
         batch-timeout-ms 1000}}]
  (cond-> {:max-batch-size max-batch-size
           :batch-timeout-ms batch-timeout-ms}
    parallelism (assoc :parallelism parallelism)))

(defn rate-limit-config
  "Create rate limiting configuration for ML models.

  Options:
    :requests-per-minute - Maximum requests per minute (optional)
    :tokens-per-minute   - Maximum tokens per minute (optional)

  Returns a map that can be merged into model config.

  Example:
    (openai-model (merge
                    {:api-key \"sk-xxx\" :model \"gpt-4o-mini\"}
                    (rate-limit-config {:requests-per-minute 60})))"
  [{:keys [requests-per-minute tokens-per-minute]}]
  (cond-> {}
    requests-per-minute (assoc :rate-limit-rpm requests-per-minute)
    tokens-per-minute (assoc :rate-limit-tpm tokens-per-minute)))
