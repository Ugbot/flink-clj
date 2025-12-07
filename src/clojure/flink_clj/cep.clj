(ns flink-clj.cep
  "Complex Event Processing (CEP) for pattern matching on DataStreams.

  CEP allows you to detect patterns in event streams using a declarative,
  data-driven API that feels natural in Clojure.

  Patterns are built using simple Clojure maps and vectors:

  Example:
    (require '[flink-clj.cep :as cep])

    ;; Define a pattern: login followed by purchase within 10 minutes
    (def fraud-pattern
      (cep/pattern
        [:start {:where #'login-event?}]
        [:middle {:where #'add-to-cart?
                  :quantifier :one-or-more
                  :consecutive? true}]
        [:end {:where #'purchase?
               :contiguity :followed-by}]
        {:within [10 :minutes]}))

    ;; Apply pattern and process matches
    (-> event-stream
        (cep/detect fraud-pattern)
        (cep/select (fn [matches]
                      {:user (-> matches :start first :user-id)
                       :items (map :item-id (:middle matches))
                       :total (-> matches :end first :amount)})))

  The data-driven approach makes patterns composable and inspectable."
  (:require [flink-clj.impl.functions :as impl])
  (:import [java.time Duration]
           [org.apache.flink.streaming.api.datastream DataStream KeyedStream]))

;; =============================================================================
;; Dynamic Class Loading for CEP
;; =============================================================================

(defn- cep-available?
  "Check if Flink CEP is available on the classpath."
  []
  (try
    (Class/forName "org.apache.flink.cep.CEP")
    true
    (catch ClassNotFoundException _ false)))

(defn- require-cep!
  "Throw if CEP is not available."
  []
  (when-not (cep-available?)
    (throw (ex-info "Flink CEP not available. Add flink-cep to your dependencies."
                    {:dependency "org.apache.flink/flink-cep"}))))

(defn- invoke-static [class-name method-name param-types & args]
  (let [clazz (Class/forName class-name)
        method (.getMethod clazz method-name (into-array Class param-types))]
    (.invoke method nil (into-array Object args))))

(defn- invoke-method [obj method-name param-types & args]
  (let [method (.getMethod (class obj) method-name (into-array Class param-types))]
    (.invoke method obj (into-array Object args))))

;; =============================================================================
;; Duration Helpers
;; =============================================================================

(defn- ->duration
  "Convert duration spec to java.time.Duration."
  [d]
  (cond
    (instance? Duration d) d
    (vector? d)
    (let [[n unit] d]
      (case unit
        (:ms :milliseconds) (Duration/ofMillis n)
        (:s :seconds :second) (Duration/ofSeconds n)
        (:m :minutes :minute) (Duration/ofMinutes n)
        (:h :hours :hour) (Duration/ofHours n)
        (:d :days :day) (Duration/ofDays n)
        (throw (ex-info "Unknown time unit" {:unit unit}))))
    (number? d) (Duration/ofMillis d)
    :else (throw (ex-info "Invalid duration" {:value d}))))

;; =============================================================================
;; Condition Building
;; =============================================================================

(defn- make-iterative-condition
  "Create a Flink IterativeCondition from a Clojure predicate."
  [pred-fn]
  (let [[ns-str fn-name] (impl/var->ns-name (impl/fn->var pred-fn))
        wrapper-class (Class/forName "flink_clj.CljIterativeCondition")
        constructor (.getConstructor wrapper-class (into-array Class [String String]))]
    (.newInstance constructor (into-array Object [ns-str fn-name]))))

(defn- apply-where
  "Apply a where condition to a pattern state."
  [flink-pattern where-spec]
  (cond
    ;; Single predicate
    (or (var? where-spec) (fn? where-spec))
    (let [condition (make-iterative-condition where-spec)]
      (invoke-method flink-pattern "where"
                     [(Class/forName "org.apache.flink.cep.pattern.conditions.IterativeCondition")]
                     condition))

    ;; Map with :or/:and combinator
    (map? where-spec)
    (cond
      (:or where-spec)
      (reduce (fn [p pred]
                (let [condition (make-iterative-condition pred)]
                  (invoke-method p "or"
                                 [(Class/forName "org.apache.flink.cep.pattern.conditions.IterativeCondition")]
                                 condition)))
              flink-pattern
              (:or where-spec))

      (:and where-spec)
      (reduce (fn [p pred]
                (let [condition (make-iterative-condition pred)]
                  (invoke-method p "where"
                                 [(Class/forName "org.apache.flink.cep.pattern.conditions.IterativeCondition")]
                                 condition)))
              flink-pattern
              (:and where-spec))

      :else flink-pattern)

    :else flink-pattern))

;; =============================================================================
;; Quantifier Application
;; =============================================================================

(defn- apply-quantifier
  "Apply quantifier to a pattern state."
  [flink-pattern quantifier-spec]
  (cond
    (nil? quantifier-spec) flink-pattern

    (= :one-or-more quantifier-spec)
    (invoke-method flink-pattern "oneOrMore" [])

    (= :optional quantifier-spec)
    (invoke-method flink-pattern "optional" [])

    (number? quantifier-spec)
    (invoke-method flink-pattern "times" [Integer/TYPE] (int quantifier-spec))

    (vector? quantifier-spec)
    (let [[op & args] quantifier-spec]
      (case op
        :times (if (= 1 (count args))
                 (invoke-method flink-pattern "times" [Integer/TYPE] (int (first args)))
                 (invoke-method flink-pattern "times" [Integer/TYPE Integer/TYPE]
                                (int (first args)) (int (second args))))
        :times-or-more (invoke-method flink-pattern "timesOrMore" [Integer/TYPE] (int (first args)))
        :one-or-more (invoke-method flink-pattern "oneOrMore" [])
        :optional (invoke-method flink-pattern "optional" [])
        flink-pattern))

    (map? quantifier-spec)
    (let [{:keys [times times-or-more one-or-more optional greedy consecutive]} quantifier-spec
          p (cond
              one-or-more (invoke-method flink-pattern "oneOrMore" [])
              times-or-more (invoke-method flink-pattern "timesOrMore" [Integer/TYPE] (int times-or-more))
              (vector? times) (invoke-method flink-pattern "times" [Integer/TYPE Integer/TYPE]
                                             (int (first times)) (int (second times)))
              times (invoke-method flink-pattern "times" [Integer/TYPE] (int times))
              optional (invoke-method flink-pattern "optional" [])
              :else flink-pattern)
          p (if greedy (invoke-method p "greedy" []) p)
          p (if consecutive (invoke-method p "consecutive" []) p)]
      p)

    :else flink-pattern))

;; =============================================================================
;; State Options Application
;; =============================================================================

(defn- apply-until
  "Apply until (stop) condition."
  [flink-pattern until-fn]
  (if until-fn
    (let [condition (make-iterative-condition until-fn)]
      (invoke-method flink-pattern "until"
                     [(Class/forName "org.apache.flink.cep.pattern.conditions.IterativeCondition")]
                     condition))
    flink-pattern))

(defn- apply-state-options
  "Apply all options to a pattern state."
  [flink-pattern {:keys [where quantifier until consecutive? greedy? allow-combinations?]}]
  (-> flink-pattern
      (cond-> where (apply-where where))
      (cond-> quantifier (apply-quantifier quantifier))
      (cond-> until (apply-until until))
      (cond-> consecutive? (invoke-method "consecutive" []))
      (cond-> greedy? (invoke-method "greedy" []))
      (cond-> allow-combinations? (invoke-method "allowCombinations" []))))

;; =============================================================================
;; Pattern State Builders
;; =============================================================================

(defn- add-pattern-state
  "Add a new state to the pattern with the specified contiguity."
  [flink-pattern state-name contiguity options]
  (let [contiguity (or contiguity :followed-by)
        new-state (case contiguity
                    :next (invoke-method flink-pattern "next" [String] state-name)
                    :followed-by (invoke-method flink-pattern "followedBy" [String] state-name)
                    :followed-by-any (invoke-method flink-pattern "followedByAny" [String] state-name)
                    :not-next (invoke-method flink-pattern "notNext" [String] state-name)
                    :not-followed-by (invoke-method flink-pattern "notFollowedBy" [String] state-name))]
    (apply-state-options new-state options)))

;; =============================================================================
;; Pattern DSL
;; =============================================================================

(defn pattern
  "Create a CEP pattern from a declarative specification.

  Pattern states are specified as vectors of [name options]:
    [:state-name {:where pred-fn
                  :contiguity :followed-by  ; or :next, :followed-by-any
                  :quantifier :one-or-more  ; or number, [:times n], etc.
                  :consecutive? true
                  :greedy? true
                  :until pred-fn}]

  The last argument can be global options:
    {:within [10 :minutes]}

  Contiguity types:
    :next           - strict, no events between
    :followed-by    - relaxed, events can appear between (default)
    :followed-by-any - non-deterministic relaxed
    :not-next       - negative strict
    :not-followed-by - negative relaxed

  Quantifiers:
    :one-or-more
    :optional
    n               - exactly n times
    [:times n]      - exactly n times
    [:times n m]    - between n and m times
    [:times-or-more n] - at least n times
    {:times n :greedy true :consecutive true}

  Example:
    (pattern
      [:login {:where #'login-event?}]
      [:browse {:where #'browse-event?
                :contiguity :followed-by
                :quantifier :one-or-more}]
      [:purchase {:where #'purchase-event?
                  :contiguity :followed-by}]
      {:within [30 :minutes]})"
  [& specs]
  (require-cep!)
  (let [;; Separate states from global options
        global-opts (when (and (map? (last specs))
                               (not (contains? (last specs) :where)))
                      (last specs))
        states (if global-opts (butlast specs) specs)

        ;; Build the pattern
        [first-name first-opts] (first states)
        initial-pattern (invoke-static "org.apache.flink.cep.pattern.Pattern"
                                       "begin" [String] (name first-name))
        initial-with-opts (apply-state-options initial-pattern first-opts)

        ;; Add remaining states
        final-pattern (reduce (fn [p [state-name opts]]
                                (add-pattern-state p (name state-name)
                                                   (:contiguity opts) opts))
                              initial-with-opts
                              (rest states))

        ;; Apply global options
        with-global (if-let [within (:within global-opts)]
                      (let [dur (->duration within)]
                        ;; Try Duration first (Flink 2.x), fall back to Time
                        (try
                          (invoke-method final-pattern "within" [Duration] dur)
                          (catch NoSuchMethodException _
                            (let [time-class (Class/forName "org.apache.flink.streaming.api.windowing.time.Time")
                                  time-method (.getMethod time-class "milliseconds" (into-array Class [Long/TYPE]))
                                  time-val (.invoke time-method nil (into-array Object [(long (.toMillis dur))]))]
                              (invoke-method final-pattern "within" [time-class] time-val)))))
                      final-pattern)]

    ;; Return wrapped pattern
    {:type :cep-pattern
     :states (mapv first states)
     :options global-opts
     :flink-pattern with-global}))

;; =============================================================================
;; Fluent API (alternative to declarative)
;; =============================================================================

(defn begin
  "Start a new pattern. Alternative fluent API.

  Example:
    (-> (begin :start #'start-event?)
        (followed-by :middle #'middle-event?)
        (followed-by :end #'end-event?)
        (within [5 :minutes]))"
  ([state-name]
   (require-cep!)
   {:type :cep-pattern
    :states [(name state-name)]
    :flink-pattern (invoke-static "org.apache.flink.cep.pattern.Pattern"
                                  "begin" [String] (name state-name))})
  ([state-name where-fn]
   (let [p (begin state-name)]
     (update p :flink-pattern apply-where where-fn)))
  ([state-name where-fn opts]
   (let [p (begin state-name)]
     (update p :flink-pattern apply-state-options (assoc opts :where where-fn)))))

(defn followed-by
  "Add a state with relaxed contiguity (events can appear between)."
  ([pattern state-name]
   (-> pattern
       (update :states conj (name state-name))
       (update :flink-pattern #(invoke-method % "followedBy" [String] (name state-name)))))
  ([pattern state-name where-fn]
   (-> (followed-by pattern state-name)
       (update :flink-pattern apply-where where-fn)))
  ([pattern state-name where-fn opts]
   (-> (followed-by pattern state-name)
       (update :flink-pattern apply-state-options (assoc opts :where where-fn)))))

(defn followed-by-any
  "Add a state with non-deterministic relaxed contiguity."
  ([pattern state-name]
   (-> pattern
       (update :states conj (name state-name))
       (update :flink-pattern #(invoke-method % "followedByAny" [String] (name state-name)))))
  ([pattern state-name where-fn]
   (-> (followed-by-any pattern state-name)
       (update :flink-pattern apply-where where-fn)))
  ([pattern state-name where-fn opts]
   (-> (followed-by-any pattern state-name)
       (update :flink-pattern apply-state-options (assoc opts :where where-fn)))))

(defn next-state
  "Add a state with strict contiguity (no events between)."
  ([pattern state-name]
   (-> pattern
       (update :states conj (name state-name))
       (update :flink-pattern #(invoke-method % "next" [String] (name state-name)))))
  ([pattern state-name where-fn]
   (-> (next-state pattern state-name)
       (update :flink-pattern apply-where where-fn)))
  ([pattern state-name where-fn opts]
   (-> (next-state pattern state-name)
       (update :flink-pattern apply-state-options (assoc opts :where where-fn)))))

(defn not-followed-by
  "Add a negative state (this pattern should NOT occur)."
  ([pattern state-name]
   (-> pattern
       (update :states conj (name state-name))
       (update :flink-pattern #(invoke-method % "notFollowedBy" [String] (name state-name)))))
  ([pattern state-name where-fn]
   (-> (not-followed-by pattern state-name)
       (update :flink-pattern apply-where where-fn))))

(defn not-next
  "Add a strict negative state (this pattern should NOT immediately follow)."
  ([pattern state-name]
   (-> pattern
       (update :states conj (name state-name))
       (update :flink-pattern #(invoke-method % "notNext" [String] (name state-name)))))
  ([pattern state-name where-fn]
   (-> (not-next pattern state-name)
       (update :flink-pattern apply-where where-fn))))

;; =============================================================================
;; Quantifier Functions (for fluent API)
;; =============================================================================

(defn one-or-more
  "Expect one or more occurrences of the last state."
  [pattern]
  (update pattern :flink-pattern #(invoke-method % "oneOrMore" [])))

(defn times
  "Expect exactly n occurrences, or between n and m."
  ([pattern n]
   (update pattern :flink-pattern #(invoke-method % "times" [Integer/TYPE] (int n))))
  ([pattern n m]
   (update pattern :flink-pattern #(invoke-method % "times" [Integer/TYPE Integer/TYPE]
                                                  (int n) (int m)))))

(defn times-or-more
  "Expect at least n occurrences."
  [pattern n]
  (update pattern :flink-pattern #(invoke-method % "timesOrMore" [Integer/TYPE] (int n))))

(defn optional
  "Make the last state optional."
  [pattern]
  (update pattern :flink-pattern #(invoke-method % "optional" [])))

(defn greedy
  "Make the quantifier greedy (match as many as possible)."
  [pattern]
  (update pattern :flink-pattern #(invoke-method % "greedy" [])))

(defn consecutive
  "Require consecutive matches for looping patterns."
  [pattern]
  (update pattern :flink-pattern #(invoke-method % "consecutive" [])))

(defn allow-combinations
  "Allow non-deterministic combinations for looping patterns."
  [pattern]
  (update pattern :flink-pattern #(invoke-method % "allowCombinations" [])))

(defn until
  "Add a stopping condition for looping patterns."
  [pattern stop-fn]
  (update pattern :flink-pattern apply-until stop-fn))

(defn within
  "Set a time constraint for the pattern to complete."
  [pattern duration]
  (let [dur (->duration duration)]
    (update pattern :flink-pattern
            (fn [p]
              (try
                (invoke-method p "within" [Duration] dur)
                (catch NoSuchMethodException _
                  (let [time-class (Class/forName "org.apache.flink.streaming.api.windowing.time.Time")
                        time-method (.getMethod time-class "milliseconds" (into-array Class [Long/TYPE]))
                        time-val (.invoke time-method nil (into-array Object [(long (.toMillis dur))]))]
                    (invoke-method p "within" [time-class] time-val))))))))

;; =============================================================================
;; Pattern Detection
;; =============================================================================

(defn detect
  "Apply a CEP pattern to a DataStream, returning a PatternStream.

  Example:
    (detect event-stream my-pattern)"
  [stream pattern]
  (require-cep!)
  (let [flink-pattern (:flink-pattern pattern)
        cep-class (Class/forName "org.apache.flink.cep.CEP")
        data-stream-class (if (instance? KeyedStream stream)
                            KeyedStream
                            DataStream)
        pattern-class (Class/forName "org.apache.flink.cep.pattern.Pattern")
        pattern-method (.getMethod cep-class "pattern"
                                   (into-array Class [data-stream-class pattern-class]))]
    (.invoke pattern-method nil (into-array Object [stream flink-pattern]))))

;; =============================================================================
;; Pattern Selection
;; =============================================================================

(defn select
  "Transform pattern matches using a selection function.

  The function receives a map where keys are state names (keywords or strings)
  and values are vectors of matched events.

  Example:
    (defn process-match [matches]
      {:login-user (-> matches :login first :user-id)
       :purchase-amount (-> matches :purchase first :amount)})

    (-> pattern-stream
        (select #'process-match))"
  [pattern-stream select-fn]
  (require-cep!)
  (let [[ns-str fn-name] (impl/var->ns-name (impl/fn->var select-fn))
        wrapper-class (Class/forName "flink_clj.CljPatternSelectFunction")
        constructor (.getConstructor wrapper-class (into-array Class [String String]))
        wrapper (.newInstance constructor (into-array Object [ns-str fn-name]))
        select-fn-class (Class/forName "org.apache.flink.cep.PatternSelectFunction")
        select-method (.getMethod (class pattern-stream) "select"
                                  (into-array Class [select-fn-class]))]
    (.invoke select-method pattern-stream (into-array Object [wrapper]))))

(defn flat-select
  "Transform pattern matches, returning zero or more results per match.

  The function receives matches and returns a collection.

  Example:
    (defn expand-matches [matches]
      (for [item (:items matches)]
        {:item item
         :user (-> matches :user first)}))

    (-> pattern-stream
        (flat-select #'expand-matches))"
  [pattern-stream select-fn]
  (require-cep!)
  (let [[ns-str fn-name] (impl/var->ns-name (impl/fn->var select-fn))
        wrapper-class (Class/forName "flink_clj.CljPatternFlatSelectFunction")
        constructor (.getConstructor wrapper-class (into-array Class [String String]))
        wrapper (.newInstance constructor (into-array Object [ns-str fn-name]))
        flat-select-fn-class (Class/forName "org.apache.flink.cep.PatternFlatSelectFunction")
        select-method (.getMethod (class pattern-stream) "flatSelect"
                                  (into-array Class [flat-select-fn-class]))]
    (.invoke select-method pattern-stream (into-array Object [wrapper]))))

(defn process
  "Process pattern matches with full context access.

  The function receives a context map with :matches (the matched events map).
  For timed patterns, also includes timing information.

  Example:
    (defn handle-match [{:keys [matches]}]
      (let [login (-> matches :login first)
            purchase (-> matches :purchase first)]
        {:user (:user-id login)
         :fraud-score (calculate-score login purchase)}))

    (-> pattern-stream
        (process #'handle-match))"
  [pattern-stream process-fn]
  (require-cep!)
  (let [[ns-str fn-name] (impl/var->ns-name (impl/fn->var process-fn))
        wrapper-class (Class/forName "flink_clj.CljPatternProcessFunction")
        constructor (.getConstructor wrapper-class (into-array Class [String String]))
        wrapper (.newInstance constructor (into-array Object [ns-str fn-name]))
        process-fn-class (Class/forName "org.apache.flink.cep.functions.PatternProcessFunction")
        process-method (.getMethod (class pattern-stream) "process"
                                   (into-array Class [process-fn-class]))]
    (.invoke process-method pattern-stream (into-array Object [wrapper]))))

;; =============================================================================
;; Convenience Combinators
;; =============================================================================

(defn match
  "Convenience function: detect pattern and select matches in one step.

  Example:
    (-> event-stream
        (match my-pattern #'process-match))"
  ([stream pattern select-fn]
   (-> stream
       (detect pattern)
       (select select-fn)))
  ([stream pattern select-fn {:keys [flat?]}]
   (let [ps (detect stream pattern)]
     (if flat?
       (flat-select ps select-fn)
       (select ps select-fn)))))

;; =============================================================================
;; Pattern Inspection (for debugging/testing)
;; =============================================================================

(defn pattern-states
  "Get the state names in a pattern (for debugging)."
  [pattern]
  (:states pattern))

(defn pattern-info
  "Get pattern metadata (for debugging)."
  [pattern]
  (select-keys pattern [:type :states :options]))
