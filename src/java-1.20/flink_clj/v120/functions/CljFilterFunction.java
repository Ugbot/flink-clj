package flink_clj.v120.functions;

import clojure.lang.IFn;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import flink_clj.ClojureFunctionSupport;

/**
 * Flink 1.20 FilterFunction that delegates to a Clojure predicate.
 */
public class CljFilterFunction<T> extends RichFilterFunction<T> {

    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private transient IFn filterFn;

    public CljFilterFunction(String namespace, String fnName) {
        this.namespace = namespace;
        this.fnName = fnName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.filterFn = ClojureFunctionSupport.loadFunction(namespace, fnName);
    }

    @Override
    public boolean filter(T value) throws Exception {
        Object result = filterFn.invoke(value);
        if (result instanceof Boolean) {
            return (Boolean) result;
        }
        // Clojure truthiness: nil and false are falsy, everything else is truthy
        return result != null && !Boolean.FALSE.equals(result);
    }
}
