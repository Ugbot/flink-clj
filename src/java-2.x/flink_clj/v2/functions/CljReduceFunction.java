package flink_clj.v2.functions;

import clojure.lang.IFn;
import org.apache.flink.api.common.functions.ReduceFunction;
import flink_clj.ClojureFunctionSupport;

/**
 * Flink 2.x ReduceFunction that delegates to a Clojure function.
 *
 * The Clojure function should take two arguments and return a combined result.
 */
public class CljReduceFunction<T> implements ReduceFunction<T> {

    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private transient IFn reduceFn;

    public CljReduceFunction(String namespace, String fnName) {
        this.namespace = namespace;
        this.fnName = fnName;
    }

    private void ensureInitialized() {
        if (reduceFn == null) {
            this.reduceFn = ClojureFunctionSupport.loadFunction(namespace, fnName);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T reduce(T value1, T value2) throws Exception {
        ensureInitialized();
        return (T) reduceFn.invoke(value1, value2);
    }
}
