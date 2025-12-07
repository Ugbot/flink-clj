(ns flink-clj.tuple
  "Tuple utilities for positional data in Flink.

  Flink Tuples are typed, positional data structures accessed via f0, f1, etc.
  They're efficient for keyed operations and built-in aggregations.

  Creating tuples:
    (tuple2 \"key\" 42)
    (tuple3 \"key\" 42 3.14)

  Converting:
    (->vec t)           ; to Clojure vector
    (->map t [:a :b])   ; to Clojure map with given keys"
  (:import [org.apache.flink.api.java.tuple
            Tuple Tuple0 Tuple1 Tuple2 Tuple3 Tuple4 Tuple5
            Tuple6 Tuple7 Tuple8 Tuple9 Tuple10
            Tuple11 Tuple12 Tuple13 Tuple14 Tuple15
            Tuple16 Tuple17 Tuple18 Tuple19 Tuple20
            Tuple21 Tuple22 Tuple23 Tuple24 Tuple25]))

;; =============================================================================
;; Tuple Constructors
;; =============================================================================

(defn tuple0
  "Create a Tuple0 (empty tuple)."
  []
  (Tuple0.))

(defn tuple1
  "Create a Tuple1."
  [f0]
  (Tuple1. f0))

(defn tuple2
  "Create a Tuple2."
  [f0 f1]
  (Tuple2. f0 f1))

(defn tuple3
  "Create a Tuple3."
  [f0 f1 f2]
  (Tuple3. f0 f1 f2))

(defn tuple4
  "Create a Tuple4."
  [f0 f1 f2 f3]
  (Tuple4. f0 f1 f2 f3))

(defn tuple5
  "Create a Tuple5."
  [f0 f1 f2 f3 f4]
  (Tuple5. f0 f1 f2 f3 f4))

(defn tuple6
  "Create a Tuple6."
  [f0 f1 f2 f3 f4 f5]
  (Tuple6. f0 f1 f2 f3 f4 f5))

(defn tuple7
  "Create a Tuple7."
  [f0 f1 f2 f3 f4 f5 f6]
  (Tuple7. f0 f1 f2 f3 f4 f5 f6))

(defn tuple8
  "Create a Tuple8."
  [f0 f1 f2 f3 f4 f5 f6 f7]
  (Tuple8. f0 f1 f2 f3 f4 f5 f6 f7))

(defn tuple9
  "Create a Tuple9."
  [f0 f1 f2 f3 f4 f5 f6 f7 f8]
  (Tuple9. f0 f1 f2 f3 f4 f5 f6 f7 f8))

(defn tuple10
  "Create a Tuple10."
  [f0 f1 f2 f3 f4 f5 f6 f7 f8 f9]
  (Tuple10. f0 f1 f2 f3 f4 f5 f6 f7 f8 f9))

;; For tuples 11-25, use the generic tuple function

(defn tuple
  "Create a Tuple with the given values.

  Supports 0-25 fields. Prefer specific tuple2, tuple3, etc. for
  compile-time type checking when possible.

  Example:
    (tuple \"a\" 1 2.0 true)  ; => Tuple4"
  [& values]
  (let [n (count values)]
    (case n
      0 (Tuple0.)
      1 (Tuple1. (nth values 0))
      2 (Tuple2. (nth values 0) (nth values 1))
      3 (Tuple3. (nth values 0) (nth values 1) (nth values 2))
      4 (Tuple4. (nth values 0) (nth values 1) (nth values 2) (nth values 3))
      5 (apply tuple5 values)
      6 (apply tuple6 values)
      7 (apply tuple7 values)
      8 (apply tuple8 values)
      9 (apply tuple9 values)
      10 (apply tuple10 values)
      (if (<= n 25)
        (let [t (Tuple/newInstance n)]
          (doseq [i (range n)]
            (.setField t (nth values i) i))
          t)
        (throw (ex-info "Tuple supports at most 25 fields"
                        {:count n}))))))

;; =============================================================================
;; Tuple Access
;; =============================================================================

(defn get-field
  "Get a field from a Tuple by index."
  [^Tuple t index]
  (.getField t (int index)))

(defn set-field!
  "Set a field in a Tuple (mutates the tuple)."
  [^Tuple t index value]
  (.setField t value (int index))
  t)

(defn arity
  "Get the number of fields in a Tuple."
  [^Tuple t]
  (.getArity t))

;; =============================================================================
;; Conversions
;; =============================================================================

(defn ->vec
  "Convert a Tuple to a Clojure vector."
  [^Tuple t]
  (vec (for [i (range (.getArity t))]
         (.getField t i))))

(defn ->map
  "Convert a Tuple to a Clojure map with given keys.

  Example:
    (->map (tuple2 \"Alice\" 30) [:name :age])
    => {:name \"Alice\" :age 30}"
  [^Tuple t ks]
  (zipmap ks (->vec t)))

(defn copy-tuple
  "Create a copy of a Tuple."
  [^Tuple t]
  (let [n (.getArity t)
        new-t (Tuple/newInstance n)]
    (doseq [i (range n)]
      (.setField new-t (.getField t i) i))
    new-t))

;; =============================================================================
;; Tuple2 Convenience (most common case)
;; =============================================================================

(defn fst
  "Get the first element of a Tuple2."
  [^Tuple2 t]
  (.-f0 t))

(defn snd
  "Get the second element of a Tuple2."
  [^Tuple2 t]
  (.-f1 t))

(defn swap
  "Swap the elements of a Tuple2."
  [^Tuple2 t]
  (Tuple2. (.-f1 t) (.-f0 t)))

(defn map-fst
  "Apply function to first element of Tuple2."
  [f ^Tuple2 t]
  (Tuple2. (f (.-f0 t)) (.-f1 t)))

(defn map-snd
  "Apply function to second element of Tuple2."
  [f ^Tuple2 t]
  (Tuple2. (.-f0 t) (f (.-f1 t))))
