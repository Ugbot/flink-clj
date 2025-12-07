package flink_clj;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;

/**
 * Custom WindowAssigner that delegates to a Clojure function.
 *
 * The Clojure function receives a map with:
 *   :element   - The incoming element
 *   :timestamp - Event timestamp
 *   :context   - Assigner context (provides current processing/watermark time)
 *
 * It should return a collection of window specs:
 *   [{:start <long> :end <long>} ...]
 *
 * Or for single window: {:start <long> :end <long>}
 */
public class CljWindowAssigner<T> extends WindowAssigner<T, TimeWindow> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private final boolean isEventTime;
    private transient IFn assignFn;
    private transient boolean initialized = false;

    // Keywords for efficiency
    private static final Keyword ELEMENT_KEY = Keyword.intern("element");
    private static final Keyword TIMESTAMP_KEY = Keyword.intern("timestamp");
    private static final Keyword CONTEXT_KEY = Keyword.intern("context");
    private static final Keyword CURRENT_PROCESSING_TIME_KEY = Keyword.intern("current-processing-time");
    private static final Keyword START_KEY = Keyword.intern("start");
    private static final Keyword END_KEY = Keyword.intern("end");

    public CljWindowAssigner(String namespace, String fnName, boolean isEventTime) {
        this.namespace = namespace;
        this.fnName = fnName;
        this.isEventTime = isEventTime;
    }

    private void ensureInitialized() {
        if (!initialized) {
            IFn require = Clojure.var("clojure.core", "require");
            require.invoke(Clojure.read(namespace));
            assignFn = Clojure.var(namespace, fnName);
            initialized = true;
        }
    }

    @Override
    public Collection<TimeWindow> assignWindows(T element, long timestamp, WindowAssignerContext context) {
        ensureInitialized();

        // Build context map for Clojure function
        // Note: getCurrentWatermark() is not available in WindowAssignerContext in all versions
        IPersistentMap contextMap = PersistentHashMap.create(
            CURRENT_PROCESSING_TIME_KEY, context.getCurrentProcessingTime()
        );

        IPersistentMap input = PersistentHashMap.create(
            ELEMENT_KEY, element,
            TIMESTAMP_KEY, timestamp,
            CONTEXT_KEY, contextMap
        );

        Object result = assignFn.invoke(input);

        // Handle result - can be single window or collection
        List<TimeWindow> windows = new ArrayList<>();

        if (result instanceof Collection) {
            for (Object windowSpec : (Collection<?>) result) {
                windows.add(windowFromSpec(windowSpec));
            }
        } else if (result instanceof IPersistentMap) {
            windows.add(windowFromSpec(result));
        } else if (result == null) {
            // No windows assigned
            return Collections.emptyList();
        } else {
            throw new RuntimeException("Window assigner must return map or collection of maps, got: " + result.getClass());
        }

        return windows;
    }

    private TimeWindow windowFromSpec(Object spec) {
        if (!(spec instanceof IPersistentMap)) {
            throw new RuntimeException("Window spec must be a map with :start and :end, got: " + spec);
        }
        IPersistentMap windowMap = (IPersistentMap) spec;

        Object startObj = windowMap.valAt(START_KEY);
        Object endObj = windowMap.valAt(END_KEY);

        if (startObj == null || endObj == null) {
            throw new RuntimeException("Window spec must have :start and :end keys");
        }

        long start = ((Number) startObj).longValue();
        long end = ((Number) endObj).longValue();

        return new TimeWindow(start, end);
    }

    @Override
    public Trigger<T, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        if (isEventTime) {
            return (Trigger<T, TimeWindow>) org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger.create();
        } else {
            return (Trigger<T, TimeWindow>) org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger.create();
        }
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return isEventTime;
    }
}
