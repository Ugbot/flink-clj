(ns flink-clj.cli.deploy.local
  "Run Flink jobs in local embedded cluster.

  This handler executes jobs using a local MiniCluster environment,
  suitable for development and testing."
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io File]
           [java.net URL URLClassLoader]
           [java.lang.reflect Method]))

;; =============================================================================
;; JAR Loading
;; =============================================================================

(defn- create-class-loader
  "Create a URLClassLoader for the JAR file."
  [^File jar]
  (let [url (.toURL (.toURI jar))
        parent (.getContextClassLoader (Thread/currentThread))]
    (URLClassLoader. (into-array URL [url]) parent)))

(defn- find-main-class
  "Find main class in JAR manifest or by convention."
  [^File jar provided-main]
  (or provided-main
      ;; Try to read from manifest
      (try
        (let [jar-file (java.util.jar.JarFile. jar)
              manifest (.getManifest jar-file)
              main-class (when manifest
                           (.getValue (.getMainAttributes manifest) "Main-Class"))]
          (.close jar-file)
          main-class)
        (catch Exception _ nil))
      ;; Default convention
      (let [name (.getName jar)]
        (-> name
            (str/replace #"-[0-9].*\.jar$" "")
            (str/replace #"-" "_")
            (str ".job")))))

;; =============================================================================
;; Local Execution
;; =============================================================================

(defn deploy!
  "Deploy and run a JAR in local embedded Flink cluster."
  [^File jar {:keys [main parallelism savepoint args]}]
  (let [main-class (find-main-class jar main)
        job-args (if (str/blank? args)
                   (into-array String [])
                   (into-array String (str/split args #"\s+")))]

    (println (format "Main class: %s" main-class))
    (println (format "Parallelism: %d" parallelism))
    (when savepoint
      (println (format "Savepoint: %s" savepoint)))
    (println)

    ;; Set up environment variables for the job
    (System/setProperty "PARALLELISM" (str parallelism))
    (when savepoint
      (System/setProperty "SAVEPOINT_PATH" savepoint))

    ;; Create class loader and load main class
    (let [loader (create-class-loader jar)]
      (try
        ;; Set context class loader
        (.setContextClassLoader (Thread/currentThread) loader)

        ;; Load and invoke main class
        (let [clazz (Class/forName main-class true loader)
              main-method (.getMethod clazz "main"
                                      (into-array Class [(Class/forName "[Ljava.lang.String;")]))]
          (println "Starting job...")
          (println)
          (.invoke main-method nil (into-array Object [job-args]))
          (println)
          (println "Job completed successfully."))
        (catch ClassNotFoundException e
          (println (format "Error: Main class not found: %s" main-class))
          (println)
          (println "Specify the correct main class with --main")
          (System/exit 1))
        (catch Exception e
          (println (format "Error: %s" (.getMessage e)))
          (.printStackTrace e)
          (System/exit 1))
        (finally
          (.close loader))))))
