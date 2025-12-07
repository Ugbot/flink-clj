package flink_clj.v120.functions;

import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import flink_clj.ClojureTypeInfo;
import flink_clj.ClojureFunctionSupport;

/**
 * Flink 1.20 FlatJoinFunction that delegates to a Clojure function.
 *
 * The Clojure function receives two elements and returns a sequence of results.
 */
public class CljFlatJoinFunction<IN1, IN2, OUT> extends RichFlatJoinFunction<IN1, IN2, OUT>
        implements ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private transient IFn joinFn;

    public CljFlatJoinFunction(String namespace, String fnName) {
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
    public void join(IN1 first, IN2 second, Collector<OUT> out) throws Exception {
        Object result = joinFn.invoke(first, second);
        if (result != null) {
            ISeq seq = RT.seq(result);
            while (seq != null) {
                out.collect((OUT) seq.first());
                seq = seq.next();
            }
        }
    }
}
