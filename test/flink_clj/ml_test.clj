(ns flink-clj.ml-test
  "Tests for ML/agents functionality."
  (:require [clojure.test :refer :all]
            [flink-clj.ml :as ml]
            [flink-clj.version :as v]))

;; =============================================================================
;; API Existence Tests
;; =============================================================================

(deftest ml-api-functions-exist-test
  (testing "Availability check functions exist"
    (is (fn? ml/ml-available?))
    (is (fn? ml/ml-info)))

  (testing "Table environment functions exist"
    (is (fn? ml/create-table-env))
    (is (fn? ml/execute-sql)))

  (testing "Model configuration functions exist"
    (is (fn? ml/openai-model))
    (is (fn? ml/azure-openai-model))
    (is (fn? ml/custom-model))
    (is (fn? ml/chat-model))
    (is (fn? ml/embedding-model))
    (is (fn? ml/ollama-model))
    (is (fn? ml/vllm-model)))

  (testing "Model registration functions exist"
    (is (fn? ml/register-model!))
    (is (fn? ml/drop-model!)))

  (testing "ML_PREDICT helper functions exist"
    (is (fn? ml/predict-sql))
    (is (fn? ml/predict-query)))

  (testing "Stream/Table conversion functions exist"
    (is (fn? ml/stream-to-table))
    (is (fn? ml/table-to-stream)))

  (testing "Configuration helper functions exist"
    (is (fn? ml/batch-config))
    (is (fn? ml/rate-limit-config))))

;; =============================================================================
;; Availability Tests
;; =============================================================================

(deftest ml-available-test
  (testing "ml-available? returns boolean"
    (is (boolean? (ml/ml-available?)))))

(deftest ml-info-test
  (testing "ml-info returns correct structure"
    (let [info (ml/ml-info)]
      (is (map? info))
      (is (contains? info :available))
      (is (contains? info :version))
      (is (contains? info :providers))
      (is (contains? info :features))
      (is (boolean? (:available info)))
      (is (string? (:version info)))
      (is (vector? (:providers info)))
      (is (map? (:features info))))))

(deftest ml-requires-flink-2-test
  (testing "ML availability requires Flink 2.x"
    (when (v/flink-1?)
      (is (false? (ml/ml-available?))
          "ML should not be available on Flink 1.x"))))

;; =============================================================================
;; Model Configuration Tests
;; =============================================================================

(deftest openai-model-test
  (testing "OpenAI model configuration"
    (let [model (ml/openai-model {:api-key "sk-xxx"
                                  :model "gpt-4o-mini"
                                  :task :chat})]
      (is (= :openai (:provider model)))
      (is (map? (:config model)))
      (is (= "sk-xxx" (get-in model [:config :api-key])))
      (is (= "gpt-4o-mini" (get-in model [:config :model])))
      (is (= "chat" (get-in model [:config :task])))))

  (testing "OpenAI model with all options"
    (let [model (ml/openai-model {:api-key "sk-xxx"
                                  :model "gpt-4o-mini"
                                  :task :chat
                                  :system-prompt "You are helpful."
                                  :temperature 0.5
                                  :max-tokens 100
                                  :max-batch-size 32
                                  :rate-limit-rpm 60})]
      (is (= "You are helpful." (get-in model [:config :system-prompt])))
      (is (= 0.5 (get-in model [:config :temperature])))
      (is (= 100 (get-in model [:config :max-tokens])))
      (is (= 32 (get-in model [:config :max-batch-size])))
      (is (= 60 (get-in model [:config :rate-limit-rpm])))))

  (testing "OpenAI model requires api-key"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
          #"requires :api-key"
          (ml/openai-model {:model "gpt-4o-mini"}))))

  (testing "OpenAI model requires model name"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
          #"requires :model"
          (ml/openai-model {:api-key "sk-xxx"})))))

(deftest azure-openai-model-test
  (testing "Azure OpenAI model configuration"
    (let [model (ml/azure-openai-model {:api-key "xxx"
                                         :endpoint "https://my.openai.azure.com"
                                         :deployment "gpt-4o"})]
      (is (= :azure-openai (:provider model)))
      (is (= "https://my.openai.azure.com" (get-in model [:config :endpoint])))
      (is (= "2024-02-15-preview" (get-in model [:config :api-version])))))

  (testing "Azure OpenAI model requires endpoint"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
          #"requires :endpoint"
          (ml/azure-openai-model {:api-key "xxx" :deployment "gpt-4o"}))))

  (testing "Azure OpenAI model requires deployment"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
          #"requires :deployment"
          (ml/azure-openai-model {:api-key "xxx" :endpoint "https://my.azure.com"})))))

(deftest custom-model-test
  (testing "Custom model configuration"
    (let [model (ml/custom-model {:endpoint "http://localhost:11434/v1"
                                  :model "llama3.2"})]
      (is (= :custom (:provider model)))
      (is (= "http://localhost:11434/v1" (get-in model [:config :endpoint])))
      (is (= "llama3.2" (get-in model [:config :model])))))

  (testing "Custom model requires endpoint"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
          #"requires :endpoint"
          (ml/custom-model {:model "llama3.2"}))))

  (testing "Custom model requires model name"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
          #"requires :model"
          (ml/custom-model {:endpoint "http://localhost:11434/v1"})))))

(deftest chat-model-test
  (testing "Chat model is shorthand for OpenAI chat"
    (let [model (ml/chat-model {:api-key "sk-xxx"})]
      (is (= :openai (:provider model)))
      (is (= "gpt-4o-mini" (get-in model [:config :model])))
      (is (= "chat" (get-in model [:config :task])))
      (is (= 0.7 (get-in model [:config :temperature]))))))

(deftest embedding-model-test
  (testing "Embedding model is shorthand for OpenAI embedding"
    (let [model (ml/embedding-model {:api-key "sk-xxx"})]
      (is (= :openai (:provider model)))
      (is (= "text-embedding-3-small" (get-in model [:config :model])))
      (is (= "embedding" (get-in model [:config :task])))))

  (testing "Embedding model with dimensions"
    (let [model (ml/embedding-model {:api-key "sk-xxx"
                                     :model "text-embedding-3-large"
                                     :dimensions 1024})]
      (is (= 1024 (get-in model [:config :dimensions]))))))

(deftest ollama-model-test
  (testing "Ollama model configuration"
    (let [model (ml/ollama-model {:model "llama3.2"})]
      (is (= :custom (:provider model)))
      (is (= "http://localhost:11434/v1" (get-in model [:config :endpoint])))
      (is (= "llama3.2" (get-in model [:config :model])))))

  (testing "Ollama model requires model name"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
          #"requires :model"
          (ml/ollama-model {})))))

(deftest vllm-model-test
  (testing "vLLM model configuration"
    (let [model (ml/vllm-model {:model "meta-llama/Llama-3.2-3B-Instruct"})]
      (is (= :custom (:provider model)))
      (is (= "http://localhost:8000/v1" (get-in model [:config :endpoint])))
      (is (= "meta-llama/Llama-3.2-3B-Instruct" (get-in model [:config :model])))))

  (testing "vLLM model requires model name"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
          #"requires :model"
          (ml/vllm-model {})))))

;; =============================================================================
;; ML_PREDICT SQL Generation Tests
;; =============================================================================

(deftest predict-sql-test
  (testing "Basic ML_PREDICT SQL"
    (let [sql (ml/predict-sql "chat_model" "prompt")]
      (is (= "ML_PREDICT('chat_model', prompt)" sql))))

  (testing "ML_PREDICT with output alias"
    (let [sql (ml/predict-sql "chat_model" "prompt" {:output-as "response"})]
      (is (= "ML_PREDICT('chat_model', prompt) AS response" sql))))

  (testing "ML_PREDICT with args"
    (let [sql (ml/predict-sql "chat_model" "prompt" {:args {:temperature 0.5}})]
      (is (.contains sql "MAP["))
      (is (.contains sql "'temperature', '0.5'")))))

(deftest predict-query-test
  (testing "Complete SELECT query generation"
    (let [sql (ml/predict-query "chat_model" "prompt" "messages")]
      (is (= "SELECT ML_PREDICT('chat_model', prompt) AS prediction FROM messages" sql))))

  (testing "Query with custom output column"
    (let [sql (ml/predict-query "chat_model" "prompt" "messages" "response")]
      (is (= "SELECT ML_PREDICT('chat_model', prompt) AS response FROM messages" sql)))))

;; =============================================================================
;; Configuration Helper Tests
;; =============================================================================

(deftest batch-config-test
  (testing "Batch config with defaults"
    (let [config (ml/batch-config {})]
      (is (= 16 (:max-batch-size config)))
      (is (= 1000 (:batch-timeout-ms config)))))

  (testing "Batch config with custom values"
    (let [config (ml/batch-config {:max-batch-size 32
                                   :batch-timeout-ms 2000
                                   :parallelism 4})]
      (is (= 32 (:max-batch-size config)))
      (is (= 2000 (:batch-timeout-ms config)))
      (is (= 4 (:parallelism config))))))

(deftest rate-limit-config-test
  (testing "Rate limit config"
    (let [config (ml/rate-limit-config {:requests-per-minute 60})]
      (is (= 60 (:rate-limit-rpm config))))
    (let [config (ml/rate-limit-config {:tokens-per-minute 10000})]
      (is (= 10000 (:rate-limit-tpm config))))
    (let [config (ml/rate-limit-config {:requests-per-minute 60
                                        :tokens-per-minute 10000})]
      (is (= 60 (:rate-limit-rpm config)))
      (is (= 10000 (:rate-limit-tpm config))))))

;; =============================================================================
;; Version Guard Tests
;; =============================================================================

(deftest ml-version-guard-test
  (testing "ML functions throw on Flink 1.x"
    (when (v/flink-1?)
      (testing "create-table-env throws"
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
              #"requires Flink ML support"
              (ml/create-table-env nil))))

      (testing "register-model! throws"
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
              #"requires Flink ML support"
              (ml/register-model! nil "model" (ml/openai-model {:api-key "x" :model "y"})))))

      (testing "drop-model! throws"
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
              #"requires Flink ML support"
              (ml/drop-model! nil "model")))))))

;; =============================================================================
;; Integration Tests (require ML API)
;; =============================================================================

(deftest ^:integration ml-model-registration-test
  (testing "ML model can be registered"
    (when (ml/ml-available?)
      ;; Would require actual table environment
      (is true "Integration test placeholder"))))

(deftest ^:integration ml-predict-execution-test
  (testing "ML_PREDICT can be executed"
    (when (ml/ml-available?)
      ;; Would require actual table environment and model
      (is true "Integration test placeholder"))))
