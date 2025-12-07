package flink_clj.v120.functions;

import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import flink_clj.ClojureTypeInfo;
import flink_clj.ClojureFunctionSupport;

/**
 * Flink 1.20 ProcessJoinFunction for interval joins that delegates to a Clojure function.
 */
public class CljProcessJoinFunction<IN1, IN2, OUT> extends ProcessJoinFunction<IN1, IN2, OUT>
        implements ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private final boolean flatOutput;
    private transient IFn processFn;

    /**
     * Create a ProcessJoinFunction.
     *
     * @param namespace Clojure namespace
     * @param fnName    Function name
     * @param flatOutput If true, treat result as sequence and emit each element
     */
    public CljProcessJoinFunction(String namespace, String fnName, boolean flatOutput) {
        this.namespace = namespace;
        this.fnName = fnName;
        this.flatOutput = flatOutput;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<OUT> getProducedType() {
        return (TypeInformation<OUT>) new ClojureTypeInfo(Object.class);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.processFn = ClojureFunctionSupport.loadFunction(namespace, fnName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processElement(IN1 left, IN2 right, Context ctx, Collector<OUT> out) throws Exception {
        Object result = processFn.invoke(left, right);
        if (result != null) {
            if (flatOutput) {
                ISeq seq = RT.seq(result);
                while (seq != null) {
                    out.collect((OUT) seq.first());
                    seq = seq.next();
                }
            } else {
                out.collect((OUT) result);
            }
        }
    }
}
