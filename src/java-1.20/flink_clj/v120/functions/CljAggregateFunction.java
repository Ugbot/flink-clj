package flink_clj.v120.functions;

import clojure.lang.IFn;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import flink_clj.ClojureTypeInfo;
import flink_clj.ClojureFunctionSupport;

/**
 * Flink 1.20 AggregateFunction that delegates to Clojure functions.
 *
 * Each phase of aggregation (create, add, get-result, merge) is handled
 * by a separate Clojure function.
 */
public class CljAggregateFunction<IN, ACC, OUT>
    implements AggregateFunction<IN, ACC, OUT>, ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    // create-accumulator function
    private final String createAccNs;
    private final String createAccName;

    // add function
    private final String addNs;
    private final String addName;

    // get-result function
    private final String getResultNs;
    private final String getResultName;

    // merge function (optional)
    private final String mergeNs;
    private final String mergeName;

    // Output type info
    private final TypeInformation<OUT> outputType;

    // Transient function references
    private transient IFn createAccFn;
    private transient IFn addFn;
    private transient IFn getResultFn;
    private transient IFn mergeFn;

    public CljAggregateFunction(
            String createAccNs, String createAccName,
            String addNs, String addName,
            String getResultNs, String getResultName,
            String mergeNs, String mergeName,
            TypeInformation<OUT> outputType) {
        this.createAccNs = createAccNs;
        this.createAccName = createAccName;
        this.addNs = addNs;
        this.addName = addName;
        this.getResultNs = getResultNs;
        this.getResultName = getResultName;
        this.mergeNs = mergeNs;
        this.mergeName = mergeName;
        this.outputType = outputType;
    }

    private void ensureInitialized() {
        if (createAccFn == null) {
            createAccFn = ClojureFunctionSupport.loadFunction(createAccNs, createAccName);
            addFn = ClojureFunctionSupport.loadFunction(addNs, addName);
            getResultFn = ClojureFunctionSupport.loadFunction(getResultNs, getResultName);
            mergeFn = ClojureFunctionSupport.loadFunctionOptional(mergeNs, mergeName);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public ACC createAccumulator() {
        ensureInitialized();
        return (ACC) createAccFn.invoke();
    }

    @Override
    @SuppressWarnings("unchecked")
    public ACC add(IN value, ACC accumulator) {
        ensureInitialized();
        return (ACC) addFn.invoke(accumulator, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public OUT getResult(ACC accumulator) {
        ensureInitialized();
        return (OUT) getResultFn.invoke(accumulator);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ACC merge(ACC a, ACC b) {
        ensureInitialized();
        if (mergeFn != null) {
            return (ACC) mergeFn.invoke(a, b);
        } else {
            // Default merge behavior: throw if not provided
            throw new UnsupportedOperationException(
                "Merge not implemented. Provide :merge function for session windows.");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<OUT> getProducedType() {
        if (outputType != null) {
            return outputType;
        }
        return (TypeInformation<OUT>) new ClojureTypeInfo(Object.class);
    }
}
