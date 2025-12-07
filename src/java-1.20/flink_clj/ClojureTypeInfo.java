package flink_clj;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * TypeInformation for Clojure types that use Nippy serialization.
 *
 * This provides Flink with the necessary type metadata for Clojure
 * persistent data structures.
 */
public class ClojureTypeInfo<T> extends TypeInformation<T> {

    private final Class<T> typeClass;

    public ClojureTypeInfo(Class<T> typeClass) {
        this.typeClass = typeClass;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<T> getTypeClass() {
        return typeClass;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        return (TypeSerializer<T>) new NippyTypeSerializer<>(typeClass);
    }

    @Override
    public String toString() {
        return "ClojureType<" + typeClass.getSimpleName() + ">";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ClojureTypeInfo) {
            ClojureTypeInfo<?> other = (ClojureTypeInfo<?>) obj;
            return typeClass.equals(other.typeClass);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return typeClass.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof ClojureTypeInfo;
    }
}
