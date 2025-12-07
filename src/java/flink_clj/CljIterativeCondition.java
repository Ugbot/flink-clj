package flink_clj;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.IPersistentMap;
import clojure.lang.PersistentHashMap;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

import java.util.List;
import java.util.ArrayList;

/**
 * Flink CEP IterativeCondition that delegates to a Clojure predicate function.
 *
 * The Clojure function receives the event value and should return a boolean.
 */
public class CljIterativeCondition<T> extends IterativeCondition<T> {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private transient IFn predicateFn;

    public CljIterativeCondition(String namespace, String fnName) {
        this.namespace = namespace;
        this.fnName = fnName;
    }

    private void ensureInitialized() {
        if (predicateFn == null) {
            predicateFn = ClojureFunctionSupport.loadFunction(namespace, fnName);
        }
    }

    @Override
    public boolean filter(T value, Context<T> ctx) throws Exception {
        ensureInitialized();
        Object result = predicateFn.invoke(value);
        return result != null && !Boolean.FALSE.equals(result);
    }
}
