package flink_clj.v2.functions;

import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;
import flink_clj.ClojureTypeInfo;
import flink_clj.ClojureFunctionSupport;

/**
 * Flink 2.x FlatMapFunction that delegates to a Clojure function.
 *
 * The Clojure function should return a sequence (or any iterable).
 */
public class CljFlatMapFunction<IN, OUT> extends RichFlatMapFunction<IN, OUT>
        implements ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private transient IFn flatMapFn;

    public CljFlatMapFunction(String namespace, String fnName) {
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
        this.flatMapFn = ClojureFunctionSupport.loadFunction(namespace, fnName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void flatMap(IN value, Collector<OUT> out) throws Exception {
        Object result = flatMapFn.invoke(value);

        if (result == null) {
            return;
        }

        ISeq seq = RT.seq(result);
        while (seq != null) {
            out.collect((OUT) seq.first());
            seq = seq.next();
        }
    }
}
