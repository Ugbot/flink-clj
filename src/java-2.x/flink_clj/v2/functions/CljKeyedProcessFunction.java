package flink_clj.v2.functions;

import clojure.lang.IFn;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import flink_clj.ClojureTypeInfo;
import flink_clj.ClojureFunctionSupport;

/**
 * Flink 2.x KeyedProcessFunction that delegates to Clojure functions.
 *
 * Supports:
 * - :process handler for element processing
 * - :on-timer handler for timer callbacks
 * - :open handler for initialization
 * - :close handler for cleanup
 */
public class CljKeyedProcessFunction<K, IN, OUT> extends KeyedProcessFunction<K, IN, OUT>
        implements ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private final String processNs;
    private final String processFnName;
    private final String onTimerNs;
    private final String onTimerFnName;
    private final String openNs;
    private final String openFnName;
    private final String closeNs;
    private final String closeFnName;

    private transient IFn processFn;
    private transient IFn onTimerFn;
    private transient IFn openFn;
    private transient IFn closeFn;

    public CljKeyedProcessFunction(
            String processNs, String processFnName,
            String onTimerNs, String onTimerFnName,
            String openNs, String openFnName,
            String closeNs, String closeFnName) {
        this.processNs = processNs;
        this.processFnName = processFnName;
        this.onTimerNs = onTimerNs;
        this.onTimerFnName = onTimerFnName;
        this.openNs = openNs;
        this.openFnName = openFnName;
        this.closeNs = closeNs;
        this.closeFnName = closeFnName;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        if (processNs != null && processFnName != null) {
            processFn = ClojureFunctionSupport.loadFunction(processNs, processFnName);
        }
        onTimerFn = ClojureFunctionSupport.loadFunctionOptional(onTimerNs, onTimerFnName);
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
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
        if (onTimerFn != null) {
            onTimerFn.invoke(ctx, timestamp, out);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<OUT> getProducedType() {
        return (TypeInformation<OUT>) new ClojureTypeInfo(Object.class);
    }
}
