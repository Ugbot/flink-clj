package flink_clj;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.Serializable;

/**
 * Custom Trigger that delegates to Clojure functions.
 *
 * The trigger namespace should define these functions:
 *   (on-element [ctx])     -> :fire, :purge, :fire-and-purge, :continue
 *   (on-processing-time [ctx]) -> :fire, :purge, :fire-and-purge, :continue
 *   (on-event-time [ctx])  -> :fire, :purge, :fire-and-purge, :continue
 *   (clear [ctx])          -> nil (cleanup resources)
 *
 * Context map contains:
 *   :element          - Current element (only in on-element)
 *   :timestamp        - Timestamp
 *   :window           - {:start <long> :end <long>}
 *   :current-watermark - Current watermark
 *   :current-processing-time - Current processing time
 *   :trigger-context  - Access to register timers
 *
 * Trigger context operations:
 *   :register-processing-time-timer - fn [time]
 *   :register-event-time-timer      - fn [time]
 *   :delete-processing-time-timer   - fn [time]
 *   :delete-event-time-timer        - fn [time]
 *   :get-partitioned-state          - fn [state-descriptor]
 */
public class CljTrigger<T> extends Trigger<T, TimeWindow> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String onElementFn;
    private final String onProcessingTimeFn;
    private final String onEventTimeFn;
    private final String clearFn;

    private transient boolean initialized = false;
    private transient IFn onElementImpl;
    private transient IFn onProcessingTimeImpl;
    private transient IFn onEventTimeImpl;
    private transient IFn clearImpl;

    // Keywords
    private static final Keyword ELEMENT_KEY = Keyword.intern("element");
    private static final Keyword TIMESTAMP_KEY = Keyword.intern("timestamp");
    private static final Keyword WINDOW_KEY = Keyword.intern("window");
    private static final Keyword START_KEY = Keyword.intern("start");
    private static final Keyword END_KEY = Keyword.intern("end");
    private static final Keyword CURRENT_WATERMARK_KEY = Keyword.intern("current-watermark");
    private static final Keyword CURRENT_PROCESSING_TIME_KEY = Keyword.intern("current-processing-time");
    private static final Keyword TRIGGER_CONTEXT_KEY = Keyword.intern("trigger-context");
    private static final Keyword REGISTER_PROCESSING_TIMER_KEY = Keyword.intern("register-processing-time-timer");
    private static final Keyword REGISTER_EVENT_TIMER_KEY = Keyword.intern("register-event-time-timer");
    private static final Keyword DELETE_PROCESSING_TIMER_KEY = Keyword.intern("delete-processing-time-timer");
    private static final Keyword DELETE_EVENT_TIMER_KEY = Keyword.intern("delete-event-time-timer");

    public CljTrigger(String namespace, String onElementFn, String onProcessingTimeFn,
                      String onEventTimeFn, String clearFn) {
        this.namespace = namespace;
        this.onElementFn = onElementFn;
        this.onProcessingTimeFn = onProcessingTimeFn;
        this.onEventTimeFn = onEventTimeFn;
        this.clearFn = clearFn;
    }

    private void ensureInitialized() {
        if (!initialized) {
            IFn require = Clojure.var("clojure.core", "require");
            require.invoke(Clojure.read(namespace));

            if (onElementFn != null) {
                onElementImpl = Clojure.var(namespace, onElementFn);
            }
            if (onProcessingTimeFn != null) {
                onProcessingTimeImpl = Clojure.var(namespace, onProcessingTimeFn);
            }
            if (onEventTimeFn != null) {
                onEventTimeImpl = Clojure.var(namespace, onEventTimeFn);
            }
            if (clearFn != null) {
                clearImpl = Clojure.var(namespace, clearFn);
            }

            initialized = true;
        }
    }

    private IPersistentMap buildContext(TimeWindow window, TriggerContext ctx) {
        IPersistentMap windowMap = PersistentHashMap.create(
            START_KEY, window.getStart(),
            END_KEY, window.getEnd()
        );

        // Create trigger context functions
        IPersistentMap triggerCtx = PersistentHashMap.create(
            REGISTER_PROCESSING_TIMER_KEY, new TimerRegistrar(ctx, false, true),
            REGISTER_EVENT_TIMER_KEY, new TimerRegistrar(ctx, true, true),
            DELETE_PROCESSING_TIMER_KEY, new TimerRegistrar(ctx, false, false),
            DELETE_EVENT_TIMER_KEY, new TimerRegistrar(ctx, true, false)
        );

        return PersistentHashMap.create(
            WINDOW_KEY, windowMap,
            CURRENT_WATERMARK_KEY, ctx.getCurrentWatermark(),
            CURRENT_PROCESSING_TIME_KEY, ctx.getCurrentProcessingTime(),
            TRIGGER_CONTEXT_KEY, triggerCtx
        );
    }

    private TriggerResult toTriggerResult(Object result) {
        if (result == null) {
            return TriggerResult.CONTINUE;
        }
        if (result instanceof Keyword) {
            String name = ((Keyword) result).getName();
            switch (name) {
                case "fire": return TriggerResult.FIRE;
                case "purge": return TriggerResult.PURGE;
                case "fire-and-purge": return TriggerResult.FIRE_AND_PURGE;
                case "continue": return TriggerResult.CONTINUE;
                default:
                    throw new RuntimeException("Unknown trigger result: " + name);
            }
        }
        if (result instanceof TriggerResult) {
            return (TriggerResult) result;
        }
        throw new RuntimeException("Invalid trigger result type: " + result.getClass());
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ensureInitialized();
        if (onElementImpl == null) {
            return TriggerResult.CONTINUE;
        }

        IPersistentMap context = buildContext(window, ctx);
        context = context.assoc(ELEMENT_KEY, element);
        context = context.assoc(TIMESTAMP_KEY, timestamp);

        Object result = onElementImpl.invoke(context);
        return toTriggerResult(result);
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        ensureInitialized();
        if (onProcessingTimeImpl == null) {
            return TriggerResult.CONTINUE;
        }

        IPersistentMap context = buildContext(window, ctx);
        context = context.assoc(TIMESTAMP_KEY, time);

        Object result = onProcessingTimeImpl.invoke(context);
        return toTriggerResult(result);
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        ensureInitialized();
        if (onEventTimeImpl == null) {
            return TriggerResult.CONTINUE;
        }

        IPersistentMap context = buildContext(window, ctx);
        context = context.assoc(TIMESTAMP_KEY, time);

        Object result = onEventTimeImpl.invoke(context);
        return toTriggerResult(result);
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ensureInitialized();
        if (clearImpl != null) {
            IPersistentMap context = buildContext(window, ctx);
            clearImpl.invoke(context);
        }
    }

    /**
     * Helper class that implements IFn for timer registration/deletion.
     */
    private static class TimerRegistrar implements IFn, Serializable {
        private final TriggerContext ctx;
        private final boolean eventTime;
        private final boolean register;

        TimerRegistrar(TriggerContext ctx, boolean eventTime, boolean register) {
            this.ctx = ctx;
            this.eventTime = eventTime;
            this.register = register;
        }

        @Override
        public Object invoke(Object time) {
            long t = ((Number) time).longValue();
            if (register) {
                if (eventTime) {
                    ctx.registerEventTimeTimer(t);
                } else {
                    ctx.registerProcessingTimeTimer(t);
                }
            } else {
                if (eventTime) {
                    ctx.deleteEventTimeTimer(t);
                } else {
                    ctx.deleteProcessingTimeTimer(t);
                }
            }
            return null;
        }

        // Required IFn methods - throw for unused arities
        @Override public Object invoke() { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e, Object f) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e, Object f, Object g) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e, Object f, Object g, Object h) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e, Object f, Object g, Object h, Object i) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e, Object f, Object g, Object h, Object i, Object j) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e, Object f, Object g, Object h, Object i, Object j, Object k) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e, Object f, Object g, Object h, Object i, Object j, Object k, Object l) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e, Object f, Object g, Object h, Object i, Object j, Object k, Object l, Object m) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e, Object f, Object g, Object h, Object i, Object j, Object k, Object l, Object m, Object n) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e, Object f, Object g, Object h, Object i, Object j, Object k, Object l, Object m, Object n, Object o) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e, Object f, Object g, Object h, Object i, Object j, Object k, Object l, Object m, Object n, Object o, Object p) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e, Object f, Object g, Object h, Object i, Object j, Object k, Object l, Object m, Object n, Object o, Object p, Object q) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e, Object f, Object g, Object h, Object i, Object j, Object k, Object l, Object m, Object n, Object o, Object p, Object q, Object r) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e, Object f, Object g, Object h, Object i, Object j, Object k, Object l, Object m, Object n, Object o, Object p, Object q, Object r, Object s) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e, Object f, Object g, Object h, Object i, Object j, Object k, Object l, Object m, Object n, Object o, Object p, Object q, Object r, Object s, Object t) { throw new UnsupportedOperationException(); }
        @Override public Object invoke(Object a, Object b, Object c, Object d, Object e, Object f, Object g, Object h, Object i, Object j, Object k, Object l, Object m, Object n, Object o, Object p, Object q, Object r, Object s, Object t, Object... more) { throw new UnsupportedOperationException(); }
        @Override public Object applyTo(clojure.lang.ISeq args) { return invoke(args.first()); }
        @Override public Object call() { throw new UnsupportedOperationException(); }
        @Override public void run() { throw new UnsupportedOperationException(); }
    }
}
