package flink_clj.v120.functions;

import clojure.lang.IFn;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import flink_clj.ClojureTypeInfo;
import flink_clj.ClojureFunctionSupport;

/**
 * Flink 1.20 JoinFunction that delegates to a Clojure function.
 *
 * The join function receives two elements (one from each stream) and
 * produces a joined result.
 */
public class CljJoinFunction<IN1, IN2, OUT> extends RichJoinFunction<IN1, IN2, OUT>
        implements ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private transient IFn joinFn;

    public CljJoinFunction(String namespace, String fnName) {
        this.namespace = namespace;
        this.fnName = fnName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<OUT> getProducedType() {
        return (TypeInformation<OUT>) new ClojureTypeInfo(Object.class);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.joinFn = ClojureFunctionSupport.loadFunction(namespace, fnName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public OUT join(IN1 first, IN2 second) throws Exception {
        return (OUT) joinFn.invoke(first, second);
    }
}
