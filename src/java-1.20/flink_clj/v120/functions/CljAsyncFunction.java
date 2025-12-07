package flink_clj.v120.functions;

import clojure.lang.IFn;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import flink_clj.ClojureTypeInfo;
import flink_clj.ClojureFunctionSupport;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Flink 1.20 AsyncFunction that delegates to a Clojure function.
 */
public class CljAsyncFunction<IN, OUT> extends RichAsyncFunction<IN, OUT>
        implements ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String fnName;
    private final boolean flatOutput;
    private transient IFn asyncFn;

    /**
     * Create an AsyncFunction.
     *
     * @param namespace  Clojure namespace
     * @param fnName     Function name
     * @param flatOutput If true, treat collection results as multiple outputs
     */
    public CljAsyncFunction(String namespace, String fnName, boolean flatOutput) {
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
        this.asyncFn = ClojureFunctionSupport.loadFunction(namespace, fnName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception {
        Object result = asyncFn.invoke(input);

        if (result == null) {
            // No output
            resultFuture.complete(Collections.emptyList());
        } else if (result instanceof CompletableFuture) {
            // Async result - wait for completion
            CompletableFuture<?> future = (CompletableFuture<?>) result;
            future.whenComplete((value, throwable) -> {
                if (throwable != null) {
                    resultFuture.completeExceptionally(
                        throwable instanceof Exception ? (Exception) throwable :
                            new Exception(throwable));
                } else {
                    completeResult(value, resultFuture);
                }
            });
        } else {
            // Synchronous result
            completeResult(result, resultFuture);
        }
    }

    @SuppressWarnings("unchecked")
    private void completeResult(Object value, ResultFuture<OUT> resultFuture) {
        if (value == null) {
            resultFuture.complete(Collections.emptyList());
        } else if (flatOutput && value instanceof Collection) {
            resultFuture.complete((Collection<OUT>) value);
        } else {
            resultFuture.complete(Collections.singletonList((OUT) value));
        }
    }

    @Override
    public void timeout(IN input, ResultFuture<OUT> resultFuture) throws Exception {
        // Default timeout behavior: complete with empty result
        // Override in subclass or use wrapper for custom timeout handling
        resultFuture.complete(Collections.emptyList());
    }
}
