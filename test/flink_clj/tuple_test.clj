(ns flink-clj.tuple-test
  "Tests for flink-clj.tuple namespace."
  (:require [clojure.test :refer :all]
            [flink-clj.tuple :as tuple])
  (:import [org.apache.flink.api.java.tuple Tuple Tuple0 Tuple1 Tuple2 Tuple3]))

(deftest tuple-constructors-test
  (testing "tuple0"
    (let [t (tuple/tuple0)]
      (is (instance? Tuple0 t))
      (is (= 0 (tuple/arity t)))))

  (testing "tuple1"
    (let [t (tuple/tuple1 "hello")]
      (is (instance? Tuple1 t))
      (is (= 1 (tuple/arity t)))
      (is (= "hello" (tuple/get-field t 0)))))

  (testing "tuple2"
    (let [t (tuple/tuple2 "key" 42)]
      (is (instance? Tuple2 t))
      (is (= 2 (tuple/arity t)))
      (is (= "key" (tuple/get-field t 0)))
      (is (= 42 (tuple/get-field t 1)))))

  (testing "tuple3"
    (let [t (tuple/tuple3 "a" 1 3.14)]
      (is (instance? Tuple3 t))
      (is (= 3 (tuple/arity t))))))

(deftest generic-tuple-test
  (testing "Generic tuple with few elements"
    (let [t (tuple/tuple "a" 1 2.0 true)]
      (is (instance? Tuple t))
      (is (= 4 (tuple/arity t)))))

  (testing "Generic tuple with more elements"
    (let [t (tuple/tuple 1 2 3 4 5 6 7 8 9 10 11)]
      (is (instance? Tuple t))
      (is (= 11 (tuple/arity t)))))

  (testing "Tuple max size check"
    (is (thrown? Exception (apply tuple/tuple (range 26))))))

(deftest tuple-access-test
  (testing "get-field"
    (let [t (tuple/tuple2 "hello" 42)]
      (is (= "hello" (tuple/get-field t 0)))
      (is (= 42 (tuple/get-field t 1)))))

  (testing "set-field! mutates"
    (let [t (tuple/tuple2 "hello" 42)]
      (tuple/set-field! t 1 100)
      (is (= 100 (tuple/get-field t 1)))))

  (testing "set-field! returns tuple for chaining"
    (let [t (tuple/tuple2 "a" 1)]
      (is (= t (tuple/set-field! t 0 "b"))))))

(deftest tuple-conversion-test
  (testing "->vec converts to vector"
    (let [t (tuple/tuple2 "key" 42)]
      (is (= ["key" 42] (tuple/->vec t)))))

  (testing "->map converts to map with keys"
    (let [t (tuple/tuple2 "Alice" 30)]
      (is (= {:name "Alice" :age 30}
             (tuple/->map t [:name :age]))))))

(deftest copy-tuple-test
  (testing "copy-tuple creates independent copy"
    (let [t1 (tuple/tuple2 "hello" 42)
          t2 (tuple/copy-tuple t1)]
      (tuple/set-field! t1 1 100)
      (is (= 100 (tuple/get-field t1 1)))
      (is (= 42 (tuple/get-field t2 1))))))

(deftest tuple2-convenience-test
  (testing "fst gets first element"
    (let [t (tuple/tuple2 "key" 42)]
      (is (= "key" (tuple/fst t)))))

  (testing "snd gets second element"
    (let [t (tuple/tuple2 "key" 42)]
      (is (= 42 (tuple/snd t)))))

  (testing "swap exchanges elements"
    (let [t (tuple/tuple2 "key" 42)
          swapped (tuple/swap t)]
      (is (= 42 (tuple/fst swapped)))
      (is (= "key" (tuple/snd swapped)))))

  (testing "map-fst applies function to first"
    (let [t (tuple/tuple2 "hello" 42)
          mapped (tuple/map-fst count t)]
      (is (= 5 (tuple/fst mapped)))
      (is (= 42 (tuple/snd mapped)))))

  (testing "map-snd applies function to second"
    (let [t (tuple/tuple2 "hello" 42)
          mapped (tuple/map-snd inc t)]
      (is (= "hello" (tuple/fst mapped)))
      (is (= 43 (tuple/snd mapped))))))
