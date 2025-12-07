(ns flink-clj.types
  "Type system for flink-clj, matching PyFlink's Types factory.

  Provides TypeInformation for all Flink-supported types. Use these
  for efficient native serialization instead of defaulting to Nippy.

  Primitive types:
    STRING, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, CHAR
    BIG-INT, BIG-DEC
    SQL-DATE, SQL-TIME, SQL-TIMESTAMP
    LOCAL-DATE, LOCAL-TIME, LOCAL-DATE-TIME, INSTANT

  Composite types:
    (ROW [INT STRING])              ; unnamed row
    (ROW [:id INT :name STRING])    ; named row
    (TUPLE [INT STRING])            ; positional tuple
    (LIST STRING)                   ; list of strings
    (MAP STRING LONG)               ; map from string to long

  Clojure fallback:
    CLOJURE                         ; any Clojure type via Nippy
    (CLOJURE-TYPE SomeClass)        ; specific class via Nippy

  Spec DSL:
    (from-spec :string)
    (from-spec [:row :id :int :name :string])
    (from-spec [:map :string [:list :int]])"
  (:import [org.apache.flink.api.common.typeinfo Types TypeInformation]
           [org.apache.flink.api.java.typeutils RowTypeInfo]
           [flink_clj ClojureTypeInfo]))

;; =============================================================================
;; Primitive Types
;; =============================================================================

(def STRING
  "String type."
  (Types/STRING))

(def BOOLEAN
  "Boolean type."
  (Types/BOOLEAN))

(def BYTE
  "Byte type (8-bit signed integer)."
  (Types/BYTE))

(def SHORT
  "Short type (16-bit signed integer)."
  (Types/SHORT))

(def INT
  "Integer type (32-bit signed integer)."
  (Types/INT))

(def LONG
  "Long type (64-bit signed integer)."
  (Types/LONG))

(def FLOAT
  "Float type (32-bit IEEE 754)."
  (Types/FLOAT))

(def DOUBLE
  "Double type (64-bit IEEE 754)."
  (Types/DOUBLE))

(def CHAR
  "Character type."
  (Types/CHAR))

;; =============================================================================
;; Big Number Types
;; =============================================================================

(def BIG-INT
  "Arbitrary precision integer (java.math.BigInteger)."
  (Types/BIG_INT))

(def BIG-DEC
  "Arbitrary precision decimal (java.math.BigDecimal)."
  (Types/BIG_DEC))

;; =============================================================================
;; Temporal Types
;; =============================================================================

(def SQL-DATE
  "SQL Date type (java.sql.Date)."
  (Types/SQL_DATE))

(def SQL-TIME
  "SQL Time type (java.sql.Time)."
  (Types/SQL_TIME))

(def SQL-TIMESTAMP
  "SQL Timestamp type (java.sql.Timestamp)."
  (Types/SQL_TIMESTAMP))

(def LOCAL-DATE
  "Local date type (java.time.LocalDate)."
  (Types/LOCAL_DATE))

(def LOCAL-TIME
  "Local time type (java.time.LocalTime)."
  (Types/LOCAL_TIME))

(def LOCAL-DATE-TIME
  "Local date-time type (java.time.LocalDateTime)."
  (Types/LOCAL_DATE_TIME))

(def INSTANT
  "Instant type (java.time.Instant)."
  (Types/INSTANT))

;; =============================================================================
;; Special Types
;; =============================================================================

(def VOID
  "Void type (for operators that don't produce output)."
  (Types/VOID))

;; =============================================================================
;; Clojure-Specific Types (Nippy serialization fallback)
;; =============================================================================

(def CLOJURE
  "Generic Clojure type using Nippy serialization.

  Use when you don't need schema information and want to pass
  arbitrary Clojure data structures. Analogous to PyFlink's
  PICKLED_BYTE_ARRAY."
  (ClojureTypeInfo. Object))

(defn CLOJURE-TYPE
  "Clojure type with specific class hint.

  Example:
    (CLOJURE-TYPE clojure.lang.PersistentVector)"
  [^Class clazz]
  (ClojureTypeInfo. clazz))

;; =============================================================================
;; Composite Types
;; =============================================================================

(defn ROW
  "Create a Row TypeInformation.

  Supports two formats:
  - Unnamed: (ROW [INT STRING DOUBLE])
  - Named:   (ROW [:id INT :name STRING :amount DOUBLE])

  Named rows allow field access by name in downstream operations.

  Examples:
    (ROW [INT STRING])
    (ROW [:id INT :name STRING])
    (ROW [:user (ROW [:id INT :name STRING]) :score DOUBLE])  ; nested"
  [fields]
  (cond
    ;; Named fields: [:id INT :name STRING ...]
    (and (sequential? fields)
         (not (empty? fields))
         (keyword? (first fields)))
    (let [pairs (partition 2 fields)
          names (mapv (comp name first) pairs)
          types (mapv second pairs)]
      (RowTypeInfo.
        (into-array TypeInformation types)
        (into-array String names)))

    ;; Unnamed fields: [INT STRING ...]
    (sequential? fields)
    (RowTypeInfo. (into-array TypeInformation fields))

    :else
    (throw (ex-info "ROW requires a vector of types or keyword-type pairs"
                    {:fields fields}))))

(defn TUPLE
  "Create a Tuple TypeInformation.

  Tuples are positional with field access via f0, f1, etc.
  Supports up to 25 fields.

  Example:
    (TUPLE [INT STRING])
    (TUPLE [LONG STRING DOUBLE])"
  [types]
  (when (or (empty? types) (> (count types) 25))
    (throw (ex-info "TUPLE requires 1-25 types" {:count (count types)})))
  (Types/TUPLE (into-array TypeInformation types)))

(defn LIST
  "Create a List TypeInformation.

  Example:
    (LIST STRING)
    (LIST (ROW [:id INT :name STRING]))"
  [element-type]
  (Types/LIST element-type))

(defn MAP
  "Create a Map TypeInformation.

  Example:
    (MAP STRING LONG)
    (MAP STRING (LIST INT))"
  [key-type value-type]
  (Types/MAP key-type value-type))

(defn PRIMITIVE-ARRAY
  "Create a primitive array TypeInformation.

  Only works with primitive types (INT, LONG, DOUBLE, etc).

  Example:
    (PRIMITIVE-ARRAY INT)   ; int[]
    (PRIMITIVE-ARRAY DOUBLE) ; double[]"
  [element-type]
  (Types/PRIMITIVE_ARRAY element-type))

(defn OBJECT-ARRAY
  "Create an object array TypeInformation.

  Example:
    (OBJECT-ARRAY STRING)
    (OBJECT-ARRAY (ROW [:id INT]))"
  [element-type]
  (Types/OBJECT_ARRAY element-type))

;; =============================================================================
;; Type Inference
;; =============================================================================

(defn type-of
  "Infer TypeInformation from a Clojure value.

  Useful for quick testing but prefer explicit types for production.

  Examples:
    (type-of 42)        ; => LONG (Clojure integers are Long)
    (type-of \"hello\") ; => STRING
    (type-of 3.14)      ; => DOUBLE
    (type-of {:a 1})    ; => CLOJURE"
  [value]
  (cond
    (nil? value) VOID
    (string? value) STRING
    (instance? Long value) LONG
    (instance? Integer value) INT
    (instance? Double value) DOUBLE
    (instance? Float value) FLOAT
    (instance? Boolean value) BOOLEAN
    (instance? Character value) CHAR
    (instance? Byte value) BYTE
    (instance? Short value) SHORT
    (instance? java.math.BigDecimal value) BIG-DEC
    (instance? java.math.BigInteger value) BIG-INT
    (instance? java.time.Instant value) INSTANT
    (instance? java.time.LocalDate value) LOCAL-DATE
    (instance? java.time.LocalTime value) LOCAL-TIME
    (instance? java.time.LocalDateTime value) LOCAL-DATE-TIME
    (instance? java.sql.Date value) SQL-DATE
    (instance? java.sql.Time value) SQL-TIME
    (instance? java.sql.Timestamp value) SQL-TIMESTAMP
    :else CLOJURE))

;; =============================================================================
;; Spec DSL - Convert keyword specs to TypeInformation
;; =============================================================================

(declare from-spec)

(defn- spec-keyword->type
  "Convert a keyword spec to TypeInformation."
  [kw]
  (case kw
    :string STRING
    :boolean BOOLEAN
    :byte BYTE
    :short SHORT
    :int INT
    :long LONG
    :float FLOAT
    :double DOUBLE
    :char CHAR
    :big-int BIG-INT
    :big-dec BIG-DEC
    :sql-date SQL-DATE
    :sql-time SQL-TIME
    :sql-timestamp SQL-TIMESTAMP
    :local-date LOCAL-DATE
    :local-time LOCAL-TIME
    :local-date-time LOCAL-DATE-TIME
    :instant INSTANT
    :void VOID
    :clojure CLOJURE
    (throw (ex-info "Unknown type spec keyword" {:keyword kw}))))

(defn- spec-row->type
  "Convert a row spec to RowTypeInfo.

  Handles nested specs recursively."
  [fields]
  (cond
    ;; Named: [:row :id :int :name :string]
    (keyword? (first fields))
    (let [pairs (partition 2 fields)
          names (mapv (comp name first) pairs)
          types (mapv #(from-spec (second %)) pairs)]
      (RowTypeInfo.
        (into-array TypeInformation types)
        (into-array String names)))

    ;; Unnamed: [:row :int :string] or [:row [:list :int] :string]
    :else
    (let [types (mapv from-spec fields)]
      (RowTypeInfo. (into-array TypeInformation types)))))

(defn from-spec
  "Convert a Clojure spec to TypeInformation.

  Supports full nesting of complex types.

  Primitive specs:
    :string, :int, :long, :double, :boolean, :float, :byte, :short, :char
    :big-int, :big-dec
    :instant, :local-date, :local-time, :local-date-time
    :sql-date, :sql-time, :sql-timestamp
    :void, :clojure

  Composite specs:
    [:row :id :int :name :string]                    ; named row
    [:row :int :string]                               ; unnamed row
    [:row :user [:row :id :int] :score :double]      ; nested row
    [:tuple :int :string]                             ; tuple
    [:list :string]                                   ; list
    [:list [:row :id :int]]                          ; list of rows
    [:map :string :long]                              ; map
    [:map :string [:list :int]]                      ; map to list
    [:array :string]                                  ; object array
    [:primitive-array :int]                           ; primitive array

  Examples:
    (from-spec :string)
    (from-spec [:row :id :int :name :string])
    (from-spec [:map :string [:list [:row :x :double :y :double]]])"
  [spec]
  (cond
    ;; Already a TypeInformation - return as-is
    (instance? TypeInformation spec)
    spec

    ;; Keyword primitive type
    (keyword? spec)
    (spec-keyword->type spec)

    ;; Vector composite type
    (vector? spec)
    (let [[type-kind & args] spec]
      (case type-kind
        :row (spec-row->type args)
        :tuple (TUPLE (mapv from-spec args))
        :list (LIST (from-spec (first args)))
        :map (MAP (from-spec (first args)) (from-spec (second args)))
        :array (OBJECT-ARRAY (from-spec (first args)))
        :primitive-array (PRIMITIVE-ARRAY (from-spec (first args)))
        ;; If first element isn't a known keyword, treat as unnamed row fields
        (throw (ex-info "Unknown composite type in spec"
                        {:type type-kind :spec spec}))))

    :else
    (throw (ex-info "Invalid type spec" {:spec spec :type (type spec)}))))

;; =============================================================================
;; Utility Functions
;; =============================================================================

(defn type-info?
  "Check if value is a TypeInformation."
  [x]
  (instance? TypeInformation x))

(defn row-type?
  "Check if TypeInformation is a Row type."
  [^TypeInformation ti]
  (instance? RowTypeInfo ti))

(defn field-names
  "Get field names from a Row TypeInformation."
  [^RowTypeInfo row-type]
  (vec (.getFieldNames row-type)))

(defn field-types
  "Get field types from a Row TypeInformation."
  [^RowTypeInfo row-type]
  (vec (for [i (range (.getArity row-type))]
         (.getTypeAt row-type i))))
