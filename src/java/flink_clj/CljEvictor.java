package flink_clj;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentVector;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import clojure.lang.PersistentVector;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Custom Evictor that delegates to Clojure functions.
 *
 * The evictor functions receive a context map:
 *   :elements - Vector of {:value ... :timestamp ...} maps
 *   :window   - {:start <long> :end <long>}
 *   :size     - Number of elements in the window
 *   :context  - Evictor context with current watermark/processing time
 *
 * Functions should return a set/vector of indices to evict, or a predicate
 * function that returns true for elements to evict.
 */
public class CljEvictor<T> implements Evictor<T, TimeWindow>, Serializable {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String evictBeforeFn;
    private final String evictAfterFn;

    private transient boolean initialized = false;
    private transient IFn evictBeforeImpl;
    private transient IFn evictAfterImpl;

    // Keywords
    private static final Keyword ELEMENTS_KEY = Keyword.intern("elements");
    private static final Keyword WINDOW_KEY = Keyword.intern("window");
    private static final Keyword SIZE_KEY = Keyword.intern("size");
    private static final Keyword CONTEXT_KEY = Keyword.intern("context");
    private static final Keyword START_KEY = Keyword.intern("start");
    private static final Keyword END_KEY = Keyword.intern("end");
    private static final Keyword VALUE_KEY = Keyword.intern("value");
    private static final Keyword TIMESTAMP_KEY = Keyword.intern("timestamp");
    private static final Keyword CURRENT_WATERMARK_KEY = Keyword.intern("current-watermark");
    private static final Keyword CURRENT_PROCESSING_TIME_KEY = Keyword.intern("current-processing-time");

    public CljEvictor(String namespace, String evictBeforeFn, String evictAfterFn) {
        this.namespace = namespace;
        this.evictBeforeFn = evictBeforeFn;
        this.evictAfterFn = evictAfterFn;
    }

    private void ensureInitialized() {
        if (!initialized) {
            IFn require = Clojure.var("clojure.core", "require");
            require.invoke(Clojure.read(namespace));

            if (evictBeforeFn != null) {
                evictBeforeImpl = Clojure.var(namespace, evictBeforeFn);
            }
            if (evictAfterFn != null) {
                evictAfterImpl = Clojure.var(namespace, evictAfterFn);
            }

            initialized = true;
        }
    }

    private IPersistentMap buildContext(Iterable<TimestampedValue<T>> elements,
                                         int size, TimeWindow window,
                                         EvictorContext ctx) {
        // Build elements vector
        IPersistentVector elemVec = PersistentVector.EMPTY;
        for (TimestampedValue<T> tv : elements) {
            IPersistentMap elemMap = PersistentHashMap.create(
                VALUE_KEY, tv.getValue(),
                TIMESTAMP_KEY, tv.getTimestamp()
            );
            elemVec = elemVec.cons(elemMap);
        }

        // Build window map
        IPersistentMap windowMap = PersistentHashMap.create(
            START_KEY, window.getStart(),
            END_KEY, window.getEnd()
        );

        // Build context map
        IPersistentMap ctxMap = PersistentHashMap.create(
            CURRENT_WATERMARK_KEY, ctx.getCurrentWatermark(),
            CURRENT_PROCESSING_TIME_KEY, ctx.getCurrentProcessingTime()
        );

        return PersistentHashMap.create(
            ELEMENTS_KEY, elemVec,
            WINDOW_KEY, windowMap,
            SIZE_KEY, size,
            CONTEXT_KEY, ctxMap
        );
    }

    private void applyEviction(Iterable<TimestampedValue<T>> elements, Object result) {
        if (result == null) {
            return;
        }

        if (result instanceof IFn) {
            // Result is a predicate function
            IFn predicate = (IFn) result;
            Iterator<TimestampedValue<T>> iter = elements.iterator();
            while (iter.hasNext()) {
                TimestampedValue<T> tv = iter.next();
                IPersistentMap elemMap = PersistentHashMap.create(
                    VALUE_KEY, tv.getValue(),
                    TIMESTAMP_KEY, tv.getTimestamp()
                );
                Object shouldEvict = predicate.invoke(elemMap);
                if (Boolean.TRUE.equals(shouldEvict) ||
                    (shouldEvict instanceof Boolean && (Boolean) shouldEvict)) {
                    iter.remove();
                }
            }
        } else if (result instanceof java.util.Collection) {
            // Result is a collection of indices to evict
            java.util.Set<Integer> indicesToEvict = new java.util.HashSet<>();
            for (Object idx : (java.util.Collection<?>) result) {
                indicesToEvict.add(((Number) idx).intValue());
            }

            Iterator<TimestampedValue<T>> iter = elements.iterator();
            int index = 0;
            while (iter.hasNext()) {
                iter.next();
                if (indicesToEvict.contains(index)) {
                    iter.remove();
                }
                index++;
            }
        } else if (result instanceof Number) {
            // Result is a count - evict first N elements
            int count = ((Number) result).intValue();
            Iterator<TimestampedValue<T>> iter = elements.iterator();
            int evicted = 0;
            while (iter.hasNext() && evicted < count) {
                iter.next();
                iter.remove();
                evicted++;
            }
        }
    }

    @Override
    public void evictBefore(Iterable<TimestampedValue<T>> elements, int size,
                            TimeWindow window, EvictorContext ctx) {
        ensureInitialized();
        if (evictBeforeImpl == null) {
            return;
        }

        IPersistentMap context = buildContext(elements, size, window, ctx);
        Object result = evictBeforeImpl.invoke(context);
        applyEviction(elements, result);
    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<T>> elements, int size,
                           TimeWindow window, EvictorContext ctx) {
        ensureInitialized();
        if (evictAfterImpl == null) {
            return;
        }

        IPersistentMap context = buildContext(elements, size, window, ctx);
        Object result = evictAfterImpl.invoke(context);
        applyEviction(elements, result);
    }
}
