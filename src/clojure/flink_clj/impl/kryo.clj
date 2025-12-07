(ns flink-clj.impl.kryo
  "Internal: Kryo serialization registration for Clojure types.

  Registers Clojure's persistent data structures with Flink's Kryo
  using Nippy for efficient serialization."
  (:import [org.apache.flink.streaming.api.environment StreamExecutionEnvironment]
           [org.apache.flink.api.common ExecutionConfig]
           [flink_clj NippySerializer]))

(def ^:private clojure-types
  "Clojure types that need Kryo registration."
  [;; Maps
   clojure.lang.PersistentHashMap
   clojure.lang.PersistentArrayMap
   clojure.lang.PersistentTreeMap

   ;; Vectors and Lists
   clojure.lang.PersistentVector
   clojure.lang.PersistentList
   clojure.lang.PersistentList$EmptyList

   ;; Sets
   clojure.lang.PersistentHashSet
   clojure.lang.PersistentTreeSet

   ;; Sequences
   clojure.lang.LazySeq
   clojure.lang.Cons
   clojure.lang.ChunkedCons

   ;; Core types
   clojure.lang.Keyword
   clojure.lang.Symbol

   ;; Map entries
   clojure.lang.MapEntry

   ;; Numbers
   clojure.lang.Ratio
   clojure.lang.BigInt

   ;; Other common types
   clojure.lang.Var
   clojure.lang.Atom])

(defn register-clojure-types!
  "Register Clojure types with Flink's Kryo serializer.

  Uses Nippy for efficient binary serialization of Clojure's
  persistent data structures."
  [^StreamExecutionEnvironment env]
  (let [^ExecutionConfig config (.getConfig env)]
    (doseq [type clojure-types]
      (try
        (.registerTypeWithKryoSerializer config type NippySerializer)
        (catch Exception e
          ;; Some types may not be available, that's OK
          nil))))
  env)

(defn register-type!
  "Register a single type with Nippy serialization.

  Use this for custom types that contain Clojure data."
  [^StreamExecutionEnvironment env ^Class type]
  (let [^ExecutionConfig config (.getConfig env)]
    (.registerTypeWithKryoSerializer config type NippySerializer))
  env)

(defn add-default-kryo-serializer!
  "Add a custom Kryo serializer for a type.

  Use this when you need a custom serializer instead of Nippy."
  [^StreamExecutionEnvironment env ^Class type serializer-class]
  (let [^ExecutionConfig config (.getConfig env)]
    (.addDefaultKryoSerializer config type serializer-class))
  env)
