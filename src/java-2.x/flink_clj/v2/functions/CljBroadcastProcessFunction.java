package flink_clj.v2.functions;

import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;
import clojure.lang.Keyword;
import clojure.lang.IPersistentMap;
import clojure.lang.PersistentHashMap;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import flink_clj.ClojureTypeInfo;
import flink_clj.ClojureFunctionSupport;

/**
 * Flink 2.x BroadcastProcessFunction that delegates to Clojure functions.
 *
 * Uses OpenContext instead of Configuration for Flink 2.x compatibility.
 */
public class CljBroadcastProcessFunction<IN1, IN2, OUT>
        extends BroadcastProcessFunction<IN1, IN2, OUT>
        implements ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private final String processNs;
    private final String processFnName;
    private final String broadcastNs;
    private final String broadcastFnName;
    private final boolean flatOutput;

    private transient IFn processFn;
    private transient IFn broadcastFn;
    private transient Keyword elementKw;
    private transient Keyword stateKw;

    public CljBroadcastProcessFunction(
            String processNs, String processFnName,
            String broadcastNs, String broadcastFnName,
            boolean flatOutput) {
        this.processNs = processNs;
        this.processFnName = processFnName;
        this.broadcastNs = broadcastNs;
        this.broadcastFnName = broadcastFnName;
        this.flatOutput = flatOutput;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<OUT> getProducedType() {
        return (TypeInformation<OUT>) new ClojureTypeInfo(Object.class);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.processFn = ClojureFunctionSupport.loadFunctionOptional(processNs, processFnName);
        this.broadcastFn = ClojureFunctionSupport.loadFunctionOptional(broadcastNs, broadcastFnName);

        this.elementKw = Keyword.intern("element");
        this.stateKw = Keyword.intern("state");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processElement(IN1 value, ReadOnlyContext ctx, Collector<OUT> out) throws Exception {
        if (processFn != null) {
            IPersistentMap ctxMap = PersistentHashMap.create(
                elementKw, value,
                stateKw, new ReadOnlyBroadcastStateWrapper(ctx)
            );

            Object result = processFn.invoke(ctxMap);
            emitResult(result, out);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processBroadcastElement(IN2 value, Context ctx, Collector<OUT> out) throws Exception {
        if (broadcastFn != null) {
            IPersistentMap ctxMap = PersistentHashMap.create(
                elementKw, value,
                stateKw, new BroadcastStateWrapper(ctx)
            );

            Object result = broadcastFn.invoke(ctxMap);
            emitResult(result, out);
        }
    }

    @SuppressWarnings("unchecked")
    private void emitResult(Object result, Collector<OUT> out) {
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

    private class ReadOnlyBroadcastStateWrapper {
        private final ReadOnlyContext ctx;

        ReadOnlyBroadcastStateWrapper(ReadOnlyContext ctx) {
            this.ctx = ctx;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public Object get(MapStateDescriptor descriptor, Object key) throws Exception {
            return ctx.getBroadcastState(descriptor).get(key);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public boolean contains(MapStateDescriptor descriptor, Object key) throws Exception {
            return ctx.getBroadcastState(descriptor).contains(key);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public Iterable<?> entries(MapStateDescriptor descriptor) throws Exception {
            return ctx.getBroadcastState(descriptor).immutableEntries();
        }
    }

    private class BroadcastStateWrapper {
        private final Context ctx;

        BroadcastStateWrapper(Context ctx) {
            this.ctx = ctx;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public void put(MapStateDescriptor descriptor, Object key, Object value) throws Exception {
            ctx.getBroadcastState(descriptor).put(key, value);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public void remove(MapStateDescriptor descriptor, Object key) throws Exception {
            ctx.getBroadcastState(descriptor).remove(key);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public Object get(MapStateDescriptor descriptor, Object key) throws Exception {
            return ctx.getBroadcastState(descriptor).get(key);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public Iterable<?> entries(MapStateDescriptor descriptor) throws Exception {
            return ctx.getBroadcastState(descriptor).entries();
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public void clear(MapStateDescriptor descriptor) throws Exception {
            ctx.getBroadcastState(descriptor).clear();
        }
    }
}
