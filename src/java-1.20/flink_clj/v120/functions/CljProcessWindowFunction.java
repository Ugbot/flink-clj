package flink_clj.v120.functions;

import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;
import clojure.lang.Keyword;
import clojure.lang.IPersistentMap;
import clojure.lang.PersistentHashMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import flink_clj.ClojureTypeInfo;
import flink_clj.ClojureFunctionSupport;

/**
 * Flink ProcessWindowFunction that delegates to a Clojure function.
 *
 * The Clojure function receives a context map and the window elements,
 * and can return a single value, a sequence of values, or nil.
 *
 * Context map contains:
 *   :key - The window key
 *   :window - A map with :start and :end timestamps
 *   :elements - The window elements as a sequence
 */
public class CljProcessWindowFunction<IN, OUT, KEY, W extends Window>
        extends ProcessWindowFunction<IN, OUT, KEY, W>
        implements ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private final boolean flatOutput;
    private transient IFn processFn;
    private transient Keyword keyKw;
    private transient Keyword windowKw;
    private transient Keyword elementsKw;
    private transient Keyword startKw;
    private transient Keyword endKw;

    /**
     * Create a ProcessWindowFunction.
     *
     * @param namespace   Clojure namespace
     * @param fnName      Function name
     * @param flatOutput  If true, treat result as sequence and emit each element
     */
    public CljProcessWindowFunction(String namespace, String fnName, boolean flatOutput) {
        this.namespace = namespace;
        this.fnName = fnName;
        this.flatOutput = flatOutput;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<OUT> getProducedType() {
        return (TypeInformation<OUT>) new ClojureTypeInfo(Object.class);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.processFn = ClojureFunctionSupport.loadFunction(namespace, fnName);

        this.keyKw = Keyword.intern("key");
        this.windowKw = Keyword.intern("window");
        this.elementsKw = Keyword.intern("elements");
        this.startKw = Keyword.intern("start");
        this.endKw = Keyword.intern("end");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(KEY key, Context context, Iterable<IN> elements, Collector<OUT> out) throws Exception {
        // Build window info map
        W window = context.window();
        IPersistentMap windowInfo = PersistentHashMap.create(
            startKw, window.maxTimestamp() - getWindowSize(window),
            endKw, window.maxTimestamp()
        );

        // Convert elements to Clojure sequence
        ISeq elemSeq = RT.seq(elements);

        // Build context map
        IPersistentMap ctxMap = PersistentHashMap.create(
            keyKw, key,
            windowKw, windowInfo,
            elementsKw, elemSeq
        );

        // Call the Clojure function
        Object result = processFn.invoke(ctxMap);

        // Emit results
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

    private long getWindowSize(W window) {
        // Try to get window size for TimeWindow
        try {
            java.lang.reflect.Method getStart = window.getClass().getMethod("getStart");
            long start = (Long) getStart.invoke(window);
            return window.maxTimestamp() - start;
        } catch (Exception e) {
            // For global windows or others, return 0
            return 0;
        }
    }
}
