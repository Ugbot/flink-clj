(ns flink-clj.row
  "Row data structure utilities for structured streaming.

  Flink Rows are typed, positional data structures with optional field names.
  They provide efficient serialization when used with RowTypeInfo.

  Creating rows:
    (row 1 \"hello\" 3.14)           ; positional values
    (row-of {:id 1 :name \"hello\"} [:id :name]) ; from map

  Accessing fields:
    (get-field r 0)                  ; by index
    (get-field r \"name\")           ; by name (if schema has names)

  Converting:
    (row->map r [:id :name :score])  ; to Clojure map
    (row->vec r)                     ; to Clojure vector"
  (:import [org.apache.flink.types Row RowKind]))

(defn row
  "Create a Flink Row from values.

  Example:
    (row 1 \"hello\" 3.14)
    (row)  ; empty row"
  [& values]
  (if (empty? values)
    (Row/of (make-array Object 0))
    (Row/of (into-array Object values))))

(defn row-with-kind
  "Create a Flink Row with a specific RowKind.

  RowKinds:
    :insert      - Regular insert/append
    :update-before - Before image of update
    :update-after  - After image of update
    :delete      - Delete record

  Example:
    (row-with-kind :insert 1 \"hello\")"
  [kind & values]
  (let [row-kind (case kind
                   :insert RowKind/INSERT
                   :update-before RowKind/UPDATE_BEFORE
                   :update-after RowKind/UPDATE_AFTER
                   :delete RowKind/DELETE)
        r (Row/withPositions row-kind (count values))]
    (doseq [i (range (count values))]
      (.setField r i (nth values i)))
    r))

(defn row-of
  "Create a Row from a map with specified field order.

  Example:
    (row-of {:id 1 :name \"Alice\" :score 95.5} [:id :name :score])"
  [m field-order]
  (let [values (map #(get m %) field-order)]
    (apply row values)))

(defn get-field
  "Get a field from a Row by index or name.

  Example:
    (get-field r 0)
    (get-field r \"name\")"
  [^Row row field]
  (cond
    (number? field) (.getField row (int field))
    (string? field) (.getField row ^String field)
    (keyword? field) (.getField row (name field))
    :else (throw (ex-info "Field must be number, string, or keyword"
                          {:field field}))))

(defn set-field!
  "Set a field in a Row (mutates the row).

  Example:
    (set-field! r 0 42)
    (set-field! r \"name\" \"Bob\")"
  [^Row row field value]
  (cond
    (number? field) (.setField row (int field) value)
    (string? field) (.setField row ^String field value)
    (keyword? field) (.setField row (name field) value)
    :else (throw (ex-info "Field must be number, string, or keyword"
                          {:field field})))
  row)

(defn arity
  "Get the number of fields in a Row."
  [^Row row]
  (.getArity row))

(defn row-kind
  "Get the RowKind of a Row.

  Returns :insert, :update-before, :update-after, or :delete."
  [^Row row]
  (let [k (.getKind row)]
    (condp = k
      RowKind/INSERT :insert
      RowKind/UPDATE_BEFORE :update-before
      RowKind/UPDATE_AFTER :update-after
      RowKind/DELETE :delete)))

(defn row->vec
  "Convert a Row to a Clojure vector."
  [^Row row]
  (vec (for [i (range (.getArity row))]
         (.getField row i))))

(defn row->map
  "Convert a Row to a Clojure map given field names.

  Example:
    (row->map r [:id :name :score])
    => {:id 1 :name \"Alice\" :score 95.5}"
  [^Row row field-names]
  (zipmap field-names
          (for [i (range (.getArity row))]
            (.getField row i))))

(defn copy-row
  "Create a copy of a Row."
  [^Row row]
  (Row/copy row))

(defn project
  "Create a new Row with only the specified fields (by index).

  Example:
    (project r [0 2])  ; Keep fields 0 and 2"
  [^Row row indices]
  (Row/project row (int-array indices)))

(defn join-rows
  "Join two or more Rows into a single Row.

  Example:
    (join-rows r1 r2)  ; All fields of r1 followed by all fields of r2
    (join-rows r1 r2 r3)  ; All fields concatenated"
  ([^Row row1 ^Row row2]
   (Row/join row1 (into-array Row [row2])))
  ([^Row row1 ^Row row2 & more]
   (Row/join row1 (into-array Row (cons row2 more)))))
