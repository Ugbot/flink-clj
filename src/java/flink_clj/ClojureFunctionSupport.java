package flink_clj;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

/**
 * Shared utility class for initializing Clojure functions in Flink operators.
 *
 * This class provides common logic for loading Clojure namespaces and resolving
 * functions, used by all Clojure-delegating Flink functions.
 */
public final class ClojureFunctionSupport {

    private ClojureFunctionSupport() {
        // Utility class, prevent instantiation
    }

    /**
     * Load a Clojure namespace and return the specified function.
     *
     * @param namespace The Clojure namespace to require
     * @param fnName The name of the function to resolve
     * @return The resolved Clojure function
     * @throws RuntimeException if the function cannot be found
     */
    public static IFn loadFunction(String namespace, String fnName) {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read(namespace));
        IFn fn = Clojure.var(namespace, fnName);

        if (fn == null) {
            throw new RuntimeException(
                "Could not find function " + fnName + " in namespace " + namespace);
        }

        return fn;
    }

    /**
     * Load a Clojure namespace and return the specified function, allowing null.
     *
     * Used for optional functions (like on-timer callbacks).
     *
     * @param namespace The Clojure namespace to require (may be null)
     * @param fnName The name of the function to resolve (may be null)
     * @return The resolved Clojure function, or null if namespace/fnName is null
     */
    public static IFn loadFunctionOptional(String namespace, String fnName) {
        if (namespace == null || fnName == null) {
            return null;
        }
        return loadFunction(namespace, fnName);
    }
}
