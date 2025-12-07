package flink_clj;

import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;
import clojure.lang.Keyword;
import clojure.lang.IPersistentMap;
import clojure.lang.PersistentHashMap;
import clojure.lang.PersistentVector;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * Flink CEP PatternFlatSelectFunction that delegates to a Clojure function.
 *
 * The Clojure function receives a map of matches and returns a collection.
 * Each element in the collection is emitted as output.
 */
public class CljPatternFlatSelectFunction<IN, OUT> implements PatternFlatSelectFunction<IN, OUT> {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private transient IFn selectFn;

    public CljPatternFlatSelectFunction(String namespace, String fnName) {
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
    public void flatSelect(Map<String, List<IN>> pattern, Collector<OUT> out) throws Exception {
        ensureInitialized();

        // Convert Java Map<String, List> to Clojure map with keywords and vectors
        Object[] kvs = new Object[pattern.size() * 2];
        int i = 0;
        for (Map.Entry<String, List<IN>> entry : pattern.entrySet()) {
            kvs[i++] = Keyword.intern(entry.getKey());
            kvs[i++] = PersistentVector.create(entry.getValue());
        }
        IPersistentMap clojureMap = PersistentHashMap.create(kvs);

        Object result = selectFn.invoke(clojureMap);

        if (result != null) {
            ISeq seq = RT.seq(result);
            while (seq != null) {
                out.collect((OUT) seq.first());
                seq = seq.next();
            }
        }
    }
}
