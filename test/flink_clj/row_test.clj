(ns flink-clj.row-test
  "Tests for flink-clj.row namespace."
  (:require [clojure.test :refer :all]
            [flink-clj.row :as row])
  (:import [org.apache.flink.types Row RowKind]))

(deftest row-creation-test
  (testing "Create row with values"
    (let [r (row/row 1 "hello" 3.14)]
      (is (instance? Row r))
      (is (= 3 (row/arity r)))
      (is (= 1 (row/get-field r 0)))
      (is (= "hello" (row/get-field r 1)))
      (is (= 3.14 (row/get-field r 2)))))

  (testing "Create empty row"
    (let [r (row/row)]
      (is (instance? Row r))
      (is (= 0 (row/arity r))))))

(deftest row-with-kind-test
  (testing "Insert row kind"
    (let [r (row/row-with-kind :insert 1 "hello")]
      (is (= :insert (row/row-kind r)))))

  (testing "Update-before row kind"
    (let [r (row/row-with-kind :update-before 1 "hello")]
      (is (= :update-before (row/row-kind r)))))

  (testing "Update-after row kind"
    (let [r (row/row-with-kind :update-after 1 "hello")]
      (is (= :update-after (row/row-kind r)))))

  (testing "Delete row kind"
    (let [r (row/row-with-kind :delete 1 "hello")]
      (is (= :delete (row/row-kind r))))))

(deftest row-of-test
  (testing "Create row from map"
    (let [r (row/row-of {:id 1 :name "Alice" :score 95.5} [:id :name :score])]
      (is (= 3 (row/arity r)))
      (is (= 1 (row/get-field r 0)))
      (is (= "Alice" (row/get-field r 1)))
      (is (= 95.5 (row/get-field r 2))))))

(deftest get-field-test
  (testing "Get field by index"
    (let [r (row/row 1 "hello")]
      (is (= 1 (row/get-field r 0)))
      (is (= "hello" (row/get-field r 1)))))

  (testing "Get field by keyword"
    ;; Note: This requires named fields in the Row
    (let [r (row/row 1 "hello")]
      ;; Keyword access converts to string internally
      (is (thrown? Exception (row/get-field r :nonexistent))))))

(deftest set-field-test
  (testing "Set field mutates row"
    (let [r (row/row 1 "hello")]
      (row/set-field! r 0 42)
      (is (= 42 (row/get-field r 0)))))

  (testing "set-field! returns the row for chaining"
    (let [r (row/row 1 2)]
      (is (= r (row/set-field! r 0 10))))))

(deftest row-conversion-test
  (testing "Row to vector"
    (let [r (row/row 1 "hello" 3.14)]
      (is (= [1 "hello" 3.14] (row/row->vec r)))))

  (testing "Row to map"
    (let [r (row/row 1 "Alice" 95.5)]
      (is (= {:id 1 :name "Alice" :score 95.5}
             (row/row->map r [:id :name :score]))))))

(deftest copy-row-test
  (testing "Copy creates independent row"
    (let [r1 (row/row 1 "hello")
          r2 (row/copy-row r1)]
      (row/set-field! r1 0 42)
      (is (= 42 (row/get-field r1 0)))
      (is (= 1 (row/get-field r2 0))))))

(deftest project-test
  (testing "Project keeps specified fields"
    (let [r (row/row "a" "b" "c" "d")
          projected (row/project r [0 2])]
      (is (= 2 (row/arity projected)))
      (is (= "a" (row/get-field projected 0)))
      (is (= "c" (row/get-field projected 1))))))

(deftest join-rows-test
  (testing "Join concatenates rows"
    (let [r1 (row/row 1 2)
          r2 (row/row "a" "b" "c")
          joined (row/join-rows r1 r2)]
      (is (= 5 (row/arity joined)))
      (is (= 1 (row/get-field joined 0)))
      (is (= 2 (row/get-field joined 1)))
      (is (= "a" (row/get-field joined 2)))
      (is (= "b" (row/get-field joined 3)))
      (is (= "c" (row/get-field joined 4))))))
