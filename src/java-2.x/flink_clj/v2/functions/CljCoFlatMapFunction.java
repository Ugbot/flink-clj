package flink_clj.v2.functions;

import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import flink_clj.ClojureTypeInfo;
import flink_clj.ClojureFunctionSupport;

/**
 * Flink 2.x CoFlatMapFunction that delegates to two Clojure functions.
 *
 * Each function should return a sequence (or nil) of output elements.
 */
public class CljCoFlatMapFunction<IN1, IN2, OUT>
    implements CoFlatMapFunction<IN1, IN2, OUT>, ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private final String flatMap1Ns;
    private final String flatMap1Name;
    private final String flatMap2Ns;
    private final String flatMap2Name;
    private final TypeInformation<OUT> outputType;

    private transient IFn flatMap1Fn;
    private transient IFn flatMap2Fn;

    public CljCoFlatMapFunction(String flatMap1Ns, String flatMap1Name,
                                String flatMap2Ns, String flatMap2Name,
                                TypeInformation<OUT> outputType) {
        this.flatMap1Ns = flatMap1Ns;
        this.flatMap1Name = flatMap1Name;
        this.flatMap2Ns = flatMap2Ns;
        this.flatMap2Name = flatMap2Name;
        this.outputType = outputType;
    }

    private void ensureInitialized() {
        if (flatMap1Fn == null) {
            flatMap1Fn = ClojureFunctionSupport.loadFunction(flatMap1Ns, flatMap1Name);
            flatMap2Fn = ClojureFunctionSupport.loadFunction(flatMap2Ns, flatMap2Name);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void flatMap1(IN1 value, Collector<OUT> out) throws Exception {
        ensureInitialized();
        Object result = flatMap1Fn.invoke(value);
        if (result != null) {
            ISeq seq = RT.seq(result);
            while (seq != null) {
                out.collect((OUT) seq.first());
                seq = seq.next();
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void flatMap2(IN2 value, Collector<OUT> out) throws Exception {
        ensureInitialized();
        Object result = flatMap2Fn.invoke(value);
        if (result != null) {
            ISeq seq = RT.seq(result);
            while (seq != null) {
                out.collect((OUT) seq.first());
                seq = seq.next();
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<OUT> getProducedType() {
        if (outputType != null) {
            return outputType;
        }
        return (TypeInformation<OUT>) new ClojureTypeInfo(Object.class);
    }
}
