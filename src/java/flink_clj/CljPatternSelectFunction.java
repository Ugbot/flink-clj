package flink_clj;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.IPersistentMap;
import clojure.lang.PersistentHashMap;
import clojure.lang.PersistentVector;
import org.apache.flink.cep.PatternSelectFunction;

import java.util.List;
import java.util.Map;

/**
 * Flink CEP PatternSelectFunction that delegates to a Clojure function.
 *
 * The Clojure function receives a map where keys are keywords (state names)
 * and values are vectors of matched events.
 */
public class CljPatternSelectFunction<IN, OUT> implements PatternSelectFunction<IN, OUT> {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private transient IFn selectFn;

    public CljPatternSelectFunction(String namespace, String fnName) {
        this.namespace = namespace;
        this.fnName = fnName;
    }

    private void ensureInitialized() {
        if (selectFn == null) {
            selectFn = ClojureFunctionSupport.loadFunction(namespace, fnName);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public OUT select(Map<String, List<IN>> pattern) throws Exception {
        ensureInitialized();

        // Convert Java Map<String, List> to Clojure map with keywords and vectors
        Object[] kvs = new Object[pattern.size() * 2];
        int i = 0;
        for (Map.Entry<String, List<IN>> entry : pattern.entrySet()) {
            kvs[i++] = Keyword.intern(entry.getKey());
            kvs[i++] = PersistentVector.create(entry.getValue());
        }
        IPersistentMap clojureMap = PersistentHashMap.create(kvs);

        return (OUT) selectFn.invoke(clojureMap);
    }
}
