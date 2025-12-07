package flink_clj;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import java.io.Serializable;

/**
 * Custom WatermarkGenerator that delegates to Clojure functions.
 *
 * The on-event function receives a map with:
 *   :element   - The incoming element
 *   :timestamp - Event timestamp
 *   :current-watermark - Current watermark value
 *
 * It should return:
 *   - A long (new watermark value)
 *   - nil (no watermark update)
 *
 * The on-periodic function receives a map with:
 *   :current-watermark - Current watermark value
 *
 * It should return:
 *   - A long (watermark to emit)
 *   - nil (no watermark to emit)
 */
public class CljWatermarkGenerator<T> implements WatermarkGenerator<T>, Serializable {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String onEventFn;
    private final String onPeriodicFn;

    private transient boolean initialized = false;
    private transient IFn onEventImpl;
    private transient IFn onPeriodicImpl;
    private transient long currentWatermark = Long.MIN_VALUE;

    // Keywords
    private static final Keyword ELEMENT_KEY = Keyword.intern("element");
    private static final Keyword TIMESTAMP_KEY = Keyword.intern("timestamp");
    private static final Keyword CURRENT_WATERMARK_KEY = Keyword.intern("current-watermark");

    public CljWatermarkGenerator(String namespace, String onEventFn, String onPeriodicFn) {
        this.namespace = namespace;
        this.onEventFn = onEventFn;
        this.onPeriodicFn = onPeriodicFn;
    }

    private void ensureInitialized() {
        if (!initialized) {
            IFn require = Clojure.var("clojure.core", "require");
            require.invoke(Clojure.read(namespace));

            if (onEventFn != null) {
                onEventImpl = Clojure.var(namespace, onEventFn);
            }
            if (onPeriodicFn != null) {
                onPeriodicImpl = Clojure.var(namespace, onPeriodicFn);
            }

            initialized = true;
        }
    }

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        ensureInitialized();

        if (onEventImpl == null) {
            // Default behavior: track max timestamp
            currentWatermark = Math.max(currentWatermark, eventTimestamp);
            return;
        }

        IPersistentMap context = PersistentHashMap.create(
            ELEMENT_KEY, event,
            TIMESTAMP_KEY, eventTimestamp,
            CURRENT_WATERMARK_KEY, currentWatermark
        );

        Object result = onEventImpl.invoke(context);

        if (result != null) {
            long newWatermark = ((Number) result).longValue();
            if (newWatermark > currentWatermark) {
                currentWatermark = newWatermark;
            }
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        ensureInitialized();

        if (onPeriodicImpl == null) {
            // Default behavior: emit current watermark
            if (currentWatermark > Long.MIN_VALUE) {
                output.emitWatermark(new Watermark(currentWatermark));
            }
            return;
        }

        IPersistentMap context = PersistentHashMap.create(
            CURRENT_WATERMARK_KEY, currentWatermark
        );

        Object result = onPeriodicImpl.invoke(context);

        if (result != null) {
            long watermarkValue = ((Number) result).longValue();
            output.emitWatermark(new Watermark(watermarkValue));
            currentWatermark = Math.max(currentWatermark, watermarkValue);
        }
    }
}
