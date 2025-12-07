package flink_clj;

import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import clojure.lang.PersistentVector;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink CoGroupFunction that delegates to a Clojure function.
 *
 * The Clojure function receives a map with:
 *   :left  - Collection of elements from the left stream
 *   :right - Collection of elements from the right stream
 *
 * The function should return either:
 *   - A single result (emitted to output)
 *   - A sequence of results (each emitted if flatOutput is true)
 *   - nil (nothing emitted)
 */
public class CljCoGroupFunction<IN1, IN2, OUT> implements CoGroupFunction<IN1, IN2, OUT> {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private final boolean flatOutput;
    private transient IFn coGroupFn;

    private static final Keyword LEFT_KEY = Keyword.intern("left");
    private static final Keyword RIGHT_KEY = Keyword.intern("right");

    public CljCoGroupFunction(String namespace, String fnName, boolean flatOutput) {
        this.namespace = namespace;
        this.fnName = fnName;
        this.flatOutput = flatOutput;
    }

    private void ensureInitialized() {
        if (coGroupFn == null) {
            coGroupFn = ClojureFunctionSupport.loadFunction(namespace, fnName);
        }
    }

    @Override
    public void coGroup(Iterable<IN1> left, Iterable<IN2> right, Collector<OUT> out) throws Exception {
        ensureInitialized();

        // Convert iterables to Clojure vectors
        List<IN1> leftList = new ArrayList<>();
        for (IN1 item : left) {
            leftList.add(item);
        }
        PersistentVector leftVec = PersistentVector.create(leftList);

        List<IN2> rightList = new ArrayList<>();
        for (IN2 item : right) {
            rightList.add(item);
        }
        PersistentVector rightVec = PersistentVector.create(rightList);

        // Create context map
        IPersistentMap ctx = PersistentHashMap.create(
            LEFT_KEY, leftVec,
            RIGHT_KEY, rightVec
        );

        // Call the Clojure function
        Object result = coGroupFn.invoke(ctx);

        // Emit results
        if (result != null) {
            if (flatOutput && result instanceof Iterable) {
                for (Object item : (Iterable<?>) result) {
                    if (item != null) {
                        out.collect((OUT) item);
                    }
                }
            } else {
                out.collect((OUT) result);
            }
        }
    }
}
