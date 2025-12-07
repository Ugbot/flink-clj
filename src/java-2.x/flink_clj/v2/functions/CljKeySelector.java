package flink_clj.v2.functions;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import org.apache.flink.api.java.functions.KeySelector;
import flink_clj.ClojureFunctionSupport;

/**
 * Flink 2.x KeySelector that delegates to a Clojure function or keyword.
 *
 * Supports:
 * - Clojure functions: (fn [x] (:key x))
 * - Keywords: :key (acts as a function on maps)
 */
public class CljKeySelector<IN, KEY> implements KeySelector<IN, KEY> {

    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private final String keywordName;
    private transient IFn keyFn;

    /**
     * Create KeySelector from namespace and function name.
     */
    public CljKeySelector(String namespace, String fnName) {
        this.namespace = namespace;
        this.fnName = fnName;
        this.keywordName = null;
    }

    /**
     * Create KeySelector from a keyword name (for map key access).
     */
    public CljKeySelector(String keywordName) {
        this.namespace = null;
        this.fnName = null;
        this.keywordName = keywordName;
    }

    private void ensureInitialized() {
        if (keyFn == null) {
            if (keywordName != null) {
                keyFn = Keyword.intern(keywordName);
            } else {
                keyFn = ClojureFunctionSupport.loadFunction(namespace, fnName);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public KEY getKey(IN value) throws Exception {
        ensureInitialized();
        return (KEY) keyFn.invoke(value);
    }
}
