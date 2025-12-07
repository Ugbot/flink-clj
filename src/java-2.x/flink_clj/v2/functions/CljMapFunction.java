package flink_clj.v2.functions;

import clojure.lang.IFn;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import flink_clj.ClojureTypeInfo;
import flink_clj.ClojureFunctionSupport;

/**
 * Flink 2.x MapFunction that delegates to a Clojure function.
 *
 * Uses OpenContext instead of Configuration for Flink 2.x compatibility.
 */
public class CljMapFunction<IN, OUT> extends RichMapFunction<IN, OUT>
        implements ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private transient IFn mapFn;

    public CljMapFunction(String namespace, String fnName) {
        this.namespace = namespace;
        this.fnName = fnName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<OUT> getProducedType() {
        return (TypeInformation<OUT>) new ClojureTypeInfo(Object.class);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.mapFn = ClojureFunctionSupport.loadFunction(namespace, fnName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public OUT map(IN value) throws Exception {
        return (OUT) mapFn.invoke(value);
    }
}
