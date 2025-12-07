package flink_clj.v120.functions;

import clojure.lang.IFn;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import flink_clj.ClojureTypeInfo;
import flink_clj.ClojureFunctionSupport;

/**
 * Flink 1.20 CoMapFunction that delegates to two Clojure functions.
 *
 * One function handles elements from the first input stream,
 * the other handles elements from the second input stream.
 */
public class CljCoMapFunction<IN1, IN2, OUT>
    implements CoMapFunction<IN1, IN2, OUT>, ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private final String map1Ns;
    private final String map1Name;
    private final String map2Ns;
    private final String map2Name;
    private final TypeInformation<OUT> outputType;

    private transient IFn map1Fn;
    private transient IFn map2Fn;

    public CljCoMapFunction(String map1Ns, String map1Name,
                           String map2Ns, String map2Name,
                           TypeInformation<OUT> outputType) {
        this.map1Ns = map1Ns;
        this.map1Name = map1Name;
        this.map2Ns = map2Ns;
        this.map2Name = map2Name;
        this.outputType = outputType;
    }

    private void ensureInitialized() {
        if (map1Fn == null) {
            map1Fn = ClojureFunctionSupport.loadFunction(map1Ns, map1Name);
            map2Fn = ClojureFunctionSupport.loadFunction(map2Ns, map2Name);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public OUT map1(IN1 value) throws Exception {
        ensureInitialized();
        return (OUT) map1Fn.invoke(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public OUT map2(IN2 value) throws Exception {
        ensureInitialized();
        return (OUT) map2Fn.invoke(value);
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
