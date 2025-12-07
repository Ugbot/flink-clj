(ns flink-clj.types-test
  "Tests for flink-clj.types namespace - PyFlink-like type system."
  (:require [clojure.test :refer :all]
            [flink-clj.types :as t])
  (:import [org.apache.flink.api.common.typeinfo TypeInformation]
           [org.apache.flink.api.java.typeutils RowTypeInfo]))

;; =============================================================================
;; Primitive Types Tests
;; =============================================================================

(deftest primitive-types-test
  (testing "All primitive types are TypeInformation instances"
    (is (instance? TypeInformation t/STRING))
    (is (instance? TypeInformation t/BOOLEAN))
    (is (instance? TypeInformation t/BYTE))
    (is (instance? TypeInformation t/SHORT))
    (is (instance? TypeInformation t/INT))
    (is (instance? TypeInformation t/LONG))
    (is (instance? TypeInformation t/FLOAT))
    (is (instance? TypeInformation t/DOUBLE))
    (is (instance? TypeInformation t/CHAR))))

(deftest big-number-types-test
  (testing "Big number types"
    (is (instance? TypeInformation t/BIG-INT))
    (is (instance? TypeInformation t/BIG-DEC))))

(deftest temporal-types-test
  (testing "Temporal types"
    (is (instance? TypeInformation t/SQL-DATE))
    (is (instance? TypeInformation t/SQL-TIME))
    (is (instance? TypeInformation t/SQL-TIMESTAMP))
    (is (instance? TypeInformation t/LOCAL-DATE))
    (is (instance? TypeInformation t/LOCAL-TIME))
    (is (instance? TypeInformation t/LOCAL-DATE-TIME))
    (is (instance? TypeInformation t/INSTANT))))

(deftest clojure-fallback-types-test
  (testing "Clojure fallback type"
    (is (instance? TypeInformation t/CLOJURE)))
  (testing "Clojure type with class hint"
    (is (instance? TypeInformation (t/CLOJURE-TYPE clojure.lang.PersistentVector)))))

;; =============================================================================
;; Composite Types Tests
;; =============================================================================

(deftest row-type-test
  (testing "Unnamed row"
    (let [row-type (t/ROW [t/INT t/STRING t/DOUBLE])]
      (is (instance? RowTypeInfo row-type))
      (is (= 3 (.getArity row-type)))))

  (testing "Named row"
    (let [row-type (t/ROW [:id t/INT :name t/STRING :score t/DOUBLE])]
      (is (instance? RowTypeInfo row-type))
      (is (= 3 (.getArity row-type)))
      (is (= ["id" "name" "score"] (vec (.getFieldNames row-type))))))

  (testing "Nested row"
    (let [inner (t/ROW [:x t/DOUBLE :y t/DOUBLE])
          outer (t/ROW [:point inner :label t/STRING])]
      (is (instance? RowTypeInfo outer))
      (is (= 2 (.getArity outer))))))

(deftest tuple-type-test
  (testing "Tuple with multiple types"
    (let [tuple-type (t/TUPLE [t/STRING t/LONG])]
      (is (instance? TypeInformation tuple-type))))

  (testing "Tuple bounds check"
    (is (thrown? Exception (t/TUPLE [])))
    (is (thrown? Exception (t/TUPLE (repeat 26 t/INT))))))

(deftest list-type-test
  (testing "List of primitives"
    (let [list-type (t/LIST t/STRING)]
      (is (instance? TypeInformation list-type))))

  (testing "List of rows"
    (let [list-type (t/LIST (t/ROW [:id t/INT]))]
      (is (instance? TypeInformation list-type)))))

(deftest map-type-test
  (testing "Map of primitives"
    (let [map-type (t/MAP t/STRING t/LONG)]
      (is (instance? TypeInformation map-type))))

  (testing "Nested map"
    (let [map-type (t/MAP t/STRING (t/LIST t/INT))]
      (is (instance? TypeInformation map-type)))))

(deftest array-types-test
  (testing "Primitive array"
    (is (instance? TypeInformation (t/PRIMITIVE-ARRAY t/INT)))
    (is (instance? TypeInformation (t/PRIMITIVE-ARRAY t/DOUBLE))))

  (testing "Object array"
    (is (instance? TypeInformation (t/OBJECT-ARRAY t/STRING)))
    (is (instance? TypeInformation (t/OBJECT-ARRAY (t/ROW [:id t/INT]))))))

;; =============================================================================
;; Type Inference Tests
;; =============================================================================

(deftest type-of-test
  (testing "Infers types from values"
    (is (= t/STRING (t/type-of "hello")))
    (is (= t/LONG (t/type-of 42)))
    (is (= t/DOUBLE (t/type-of 3.14)))
    (is (= t/BOOLEAN (t/type-of true)))
    (is (= t/CLOJURE (t/type-of {:a 1})))))

;; =============================================================================
;; Spec DSL Tests
;; =============================================================================

(deftest from-spec-primitives-test
  (testing "Primitive keyword specs"
    (is (= t/STRING (t/from-spec :string)))
    (is (= t/INT (t/from-spec :int)))
    (is (= t/LONG (t/from-spec :long)))
    (is (= t/DOUBLE (t/from-spec :double)))
    (is (= t/BOOLEAN (t/from-spec :boolean)))
    (is (= t/INSTANT (t/from-spec :instant))))

  (testing "Unknown keyword throws"
    (is (thrown? Exception (t/from-spec :unknown-type)))))

(deftest from-spec-row-test
  (testing "Named row spec"
    (let [row-type (t/from-spec [:row :id :int :name :string])]
      (is (instance? RowTypeInfo row-type))
      (is (= ["id" "name"] (vec (.getFieldNames row-type))))))

  (testing "Nested row spec"
    (let [row-type (t/from-spec [:row :user [:row :id :int :name :string] :score :double])]
      (is (instance? RowTypeInfo row-type))
      (is (= 2 (.getArity row-type))))))

(deftest from-spec-composite-test
  (testing "Tuple spec"
    (let [tuple-type (t/from-spec [:tuple :string :long])]
      (is (instance? TypeInformation tuple-type))))

  (testing "List spec"
    (let [list-type (t/from-spec [:list :string])]
      (is (instance? TypeInformation list-type))))

  (testing "Map spec"
    (let [map-type (t/from-spec [:map :string :long])]
      (is (instance? TypeInformation map-type))))

  (testing "Array specs"
    (is (instance? TypeInformation (t/from-spec [:array :string])))
    (is (instance? TypeInformation (t/from-spec [:primitive-array :int])))))

(deftest from-spec-deep-nesting-test
  (testing "Deeply nested spec"
    (let [complex-type (t/from-spec [:map :string [:list [:row :x :double :y :double]]])]
      (is (instance? TypeInformation complex-type))))

  (testing "Multiple levels of nesting"
    (let [deep-type (t/from-spec [:row
                                   :users [:list [:row :id :int :name :string]]
                                   :metadata [:map :string :clojure]])]
      (is (instance? RowTypeInfo deep-type)))))

(deftest from-spec-passthrough-test
  (testing "TypeInformation passes through unchanged"
    (is (= t/STRING (t/from-spec t/STRING)))
    (let [row-type (t/ROW [:id t/INT])]
      (is (= row-type (t/from-spec row-type))))))

;; =============================================================================
;; Utility Functions Tests
;; =============================================================================

(deftest utility-functions-test
  (testing "type-info? predicate"
    (is (t/type-info? t/STRING))
    (is (not (t/type-info? "not a type"))))

  (testing "row-type? predicate"
    (is (t/row-type? (t/ROW [:id t/INT])))
    (is (not (t/row-type? t/STRING))))

  (testing "field-names extraction"
    (let [row-type (t/ROW [:id t/INT :name t/STRING])]
      (is (= ["id" "name"] (t/field-names row-type)))))

  (testing "field-types extraction"
    (let [row-type (t/ROW [:id t/INT :name t/STRING])]
      (is (= [t/INT t/STRING] (t/field-types row-type))))))
