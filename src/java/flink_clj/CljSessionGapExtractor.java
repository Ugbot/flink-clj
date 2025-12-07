package flink_clj;

import clojure.lang.IFn;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;

/**
 * Session window gap extractor that delegates to a Clojure function.
 *
 * The Clojure function receives the event value and should return
 * the gap duration in milliseconds (as a Long or Number).
 */
public class CljSessionGapExtractor<T> implements SessionWindowTimeGapExtractor<T> {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private transient IFn gapFn;

    public CljSessionGapExtractor(String namespace, String fnName) {
        this.namespace = namespace;
        this.fnName = fnName;
    }

    private void ensureInitialized() {
        if (gapFn == null) {
            gapFn = ClojureFunctionSupport.loadFunction(namespace, fnName);
        }
    }

    @Override
    public long extract(T element) {
        ensureInitialized();
        Object result = gapFn.invoke(element);
        if (result instanceof Number) {
            return ((Number) result).longValue();
        }
        throw new IllegalStateException(
            "Gap function must return a number (milliseconds), got: " +
            (result == null ? "null" : result.getClass().getName())
        );
    }
}
