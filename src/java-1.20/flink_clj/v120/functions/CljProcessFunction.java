package flink_clj.v120.functions;

import clojure.lang.IFn;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import flink_clj.ClojureTypeInfo;
import flink_clj.ClojureFunctionSupport;

/**
 * Flink 1.20 ProcessFunction that delegates to Clojure functions.
 *
 * Supports:
 * - :process handler for element processing
 * - :open handler for initialization
 * - :close handler for cleanup
 */
public class CljProcessFunction<IN, OUT> extends ProcessFunction<IN, OUT>
        implements ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private final String processNs;
    private final String processFnName;
    private final String openNs;
    private final String openFnName;
    private final String closeNs;
    private final String closeFnName;

    private transient IFn processFn;
    private transient IFn openFn;
    private transient IFn closeFn;

    public CljProcessFunction(
            String processNs, String processFnName,
            String openNs, String openFnName,
            String closeNs, String closeFnName) {
        this.processNs = processNs;
        this.processFnName = processFnName;
        this.openNs = openNs;
        this.openFnName = openFnName;
        this.closeNs = closeNs;
        this.closeFnName = closeFnName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        if (processNs != null && processFnName != null) {
            processFn = ClojureFunctionSupport.loadFunction(processNs, processFnName);
        }
        openFn = ClojureFunctionSupport.loadFunctionOptional(openNs, openFnName);
        closeFn = ClojureFunctionSupport.loadFunctionOptional(closeNs, closeFnName);

        if (openFn != null) {
            openFn.invoke(getRuntimeContext());
        }
    }

    @Override
    public void close() throws Exception {
        if (closeFn != null) {
            closeFn.invoke();
        }
        super.close();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processElement(IN value, Context ctx, Collector<OUT> out) throws Exception {
        if (processFn != null) {
            processFn.invoke(ctx, value, out);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<OUT> getProducedType() {
        return (TypeInformation<OUT>) new ClojureTypeInfo(Object.class);
    }
}
