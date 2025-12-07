package flink_clj.v2.functions;

import clojure.lang.IFn;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFilterFunction;
import flink_clj.ClojureFunctionSupport;

/**
 * Flink 2.x FilterFunction that delegates to a Clojure predicate.
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
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.filterFn = ClojureFunctionSupport.loadFunction(namespace, fnName);
    }

    @Override
    public boolean filter(T value) throws Exception {
        Object result = filterFn.invoke(value);
        if (result instanceof Boolean) {
            return (Boolean) result;
        }
        return result != null && !Boolean.FALSE.equals(result);
    }
}
