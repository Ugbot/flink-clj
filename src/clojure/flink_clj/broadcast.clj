(ns flink-clj.broadcast
  "Broadcast State Pattern for sharing dynamic rules/configuration.

  The broadcast state pattern allows you to share data (like rules, configs,
  or lookup tables) across all parallel instances of an operator. One stream
  is 'broadcast' to all instances, while another stream processes elements
  using the broadcast state.

  Key concepts:
  - Broadcast stream: Small stream of rules/config that all operators need
  - Data stream: Main stream of events to process
  - Broadcast state: Shared state accessible from both streams

  Access patterns:
  - In process-element: Read-only access to broadcast state
  - In process-broadcast: Read-write access to update rules/config

  Example:
    (require '[flink-clj.broadcast :as bc])
    (require '[flink-clj.state :as state])

    ;; Create state descriptor for broadcast state
    (def rules-state (state/map-state \"rules\" :string :string))

    ;; Define handlers
    (defn process-event [{:keys [element state]}]
      ;; Read rule from broadcast state (read-only)
      (let [rule (.get state rules-state (:type element))]
        (when (matches? element rule)
          {:matched true :element element :rule rule})))

    (defn update-rule [{:keys [element state]}]
      ;; Update broadcast state (read-write)
      (.put state rules-state (:rule-id element) (:rule-body element))
      nil)  ; Don't emit anything

    ;; Connect and process
    (-> data-stream
        (bc/connect-broadcast rules-stream rules-state)
        (bc/process {:process-element #'process-event
                     :process-broadcast #'update-rule}))"
  (:require [flink-clj.impl.functions :as impl]
            [flink-clj.types :as types])
  (:import [org.apache.flink.streaming.api.datastream
            DataStream BroadcastStream BroadcastConnectedStream]
           [org.apache.flink.api.common.state MapStateDescriptor]
           [org.apache.flink.api.common.typeinfo TypeInformation]))

(defn- resolve-type-info
  "Resolve a type hint to TypeInformation."
  [type-hint]
  (if (instance? TypeInformation type-hint)
    type-hint
    (types/from-spec type-hint)))

;; =============================================================================
;; Creating Broadcast Streams
;; =============================================================================

(defn as-broadcast
  "Convert a DataStream to a BroadcastStream.

  state-descriptors can be a single MapStateDescriptor or a collection of them.
  Each descriptor defines a piece of broadcast state.

  Example:
    (def rules-state (state/map-state \"rules\" :string :string))
    (def config-state (state/map-state \"config\" :string :long))

    ;; Single state
    (as-broadcast rules-stream rules-state)

    ;; Multiple states
    (as-broadcast config-stream [rules-state config-state])"
  [^DataStream stream state-descriptors]
  (let [descriptors (if (sequential? state-descriptors)
                      (into-array MapStateDescriptor state-descriptors)
                      (into-array MapStateDescriptor [state-descriptors]))]
    (.broadcast stream descriptors)))

;; =============================================================================
;; Connecting Streams
;; =============================================================================

(defn connect-broadcast
  "Connect a DataStream with a BroadcastStream for coordinated processing.

  The result can be processed with `process` to handle elements from both
  streams with shared broadcast state.

  Arguments:
    data-stream      - The main stream of events to process
    broadcast-stream - BroadcastStream (created with as-broadcast)
                       OR a DataStream + state descriptor(s)

  If broadcast-stream is a DataStream, you must also provide state-descriptors.

  Examples:
    ;; With pre-created BroadcastStream
    (def broadcast (as-broadcast rules-stream rules-state))
    (connect-broadcast events broadcast)

    ;; Shorthand - create BroadcastStream inline
    (connect-broadcast events rules-stream rules-state)"
  ([^DataStream data-stream ^BroadcastStream broadcast-stream]
   (.connect data-stream broadcast-stream))
  ([^DataStream data-stream ^DataStream broadcast-source state-descriptors]
   (let [broadcast (as-broadcast broadcast-source state-descriptors)]
     (.connect data-stream broadcast))))

;; =============================================================================
;; Processing Broadcast Connected Streams
;; =============================================================================

(defn process
  "Process a BroadcastConnectedStream with custom handlers.

  handlers is a map with:
    :process-element   - fn [{:keys [element state]}] -> output
                         Called for each element from the data stream.
                         Has READ-ONLY access to broadcast state.

    :process-broadcast - fn [{:keys [element state]}] -> output
                         Called for each element from the broadcast stream.
                         Has READ-WRITE access to broadcast state.

  The state object in the context map provides methods:
    (.get state descriptor key)      - Get value for key
    (.contains state descriptor key) - Check if key exists
    (.entries state descriptor)      - Get all entries
    (.put state descriptor key val)  - Set value (broadcast only)
    (.remove state descriptor key)   - Remove key (broadcast only)
    (.clear state descriptor)        - Clear state (broadcast only)

  Options:
    :flat?    - If true, treat collection results as multiple outputs
    :returns  - TypeInformation or spec for output type

  Example:
    (defn apply-rules [{:keys [element state]}]
      (let [rule (.get state rules-desc (:type element))]
        (when (apply-rule element rule)
          (assoc element :matched true))))

    (defn store-rule [{:keys [element state]}]
      (.put state rules-desc (:id element) (:body element))
      nil)

    (process connected-stream
      {:process-element #'apply-rules
       :process-broadcast #'store-rule})"
  ([^BroadcastConnectedStream stream handlers]
   (process stream handlers nil))
  ([^BroadcastConnectedStream stream handlers {:keys [flat? returns]}]
   (let [{:keys [process-element process-broadcast]} handlers
         wrapper (impl/make-broadcast-process-function
                   process-element process-broadcast (boolean flat?))
         result (.process stream wrapper)]
     (if returns
       (.returns result ^TypeInformation (resolve-type-info returns))
       result))))

;; =============================================================================
;; State Descriptor Helper
;; =============================================================================

(defn broadcast-state-descriptor
  "Create a MapStateDescriptor for broadcast state.

  This is a convenience wrapper around flink-clj.state/map-state that
  is specifically for broadcast state.

  Example:
    (def rules (broadcast-state-descriptor \"rules\" :string [:map :string :any]))"
  ([name key-type value-type]
   (broadcast-state-descriptor name key-type value-type nil))
  ([name key-type value-type {:keys [ttl]}]
   (let [key-type-info (resolve-type-info key-type)
         value-type-info (resolve-type-info value-type)
         desc (MapStateDescriptor. ^String name
                                   ^TypeInformation key-type-info
                                   ^TypeInformation value-type-info)]
     ;; Note: Broadcast state TTL is limited - it's stored in-memory only
     desc)))
