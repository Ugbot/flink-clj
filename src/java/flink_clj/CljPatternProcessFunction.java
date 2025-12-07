package flink_clj;

import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;
import clojure.lang.Keyword;
import clojure.lang.IPersistentMap;
import clojure.lang.PersistentHashMap;
import clojure.lang.PersistentVector;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * Flink CEP PatternProcessFunction that delegates to a Clojure function.
 *
 * The Clojure function receives a context map with :matches containing
 * the matched events map (keywords to vectors).
 */
public class CljPatternProcessFunction<IN, OUT> extends PatternProcessFunction<IN, OUT> {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private transient IFn processFn;
    private transient Keyword matchesKw;
    private transient Keyword timestampKw;

    public CljPatternProcessFunction(String namespace, String fnName) {
        this.namespace = namespace;
        this.fnName = fnName;
    }

    private void ensureInitialized() {
        if (processFn == null) {
            processFn = ClojureFunctionSupport.loadFunction(namespace, fnName);
            matchesKw = Keyword.intern("matches");
            timestampKw = Keyword.intern("timestamp");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processMatch(Map<String, List<IN>> match, Context ctx, Collector<OUT> out) throws Exception {
        ensureInitialized();

        // Convert Java Map<String, List> to Clojure map with keywords and vectors
        Object[] matchKvs = new Object[match.size() * 2];
        int i = 0;
        for (Map.Entry<String, List<IN>> entry : match.entrySet()) {
            matchKvs[i++] = Keyword.intern(entry.getKey());
            matchKvs[i++] = PersistentVector.create(entry.getValue());
        }
        IPersistentMap matchesMap = PersistentHashMap.create(matchKvs);

        // Create context map
        IPersistentMap ctxMap = PersistentHashMap.create(
            matchesKw, matchesMap,
            timestampKw, ctx.timestamp()
        );

        Object result = processFn.invoke(ctxMap);

        if (result != null) {
            // Check if result is a collection (flat output) or single value
            if (result instanceof Iterable || result instanceof ISeq) {
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
}
