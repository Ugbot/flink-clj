package flink_clj;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import clojure.java.api.Clojure;
import clojure.lang.IFn;

import java.io.IOException;

/**
 * Flink TypeSerializer that uses Nippy for Clojure types.
 */
public class NippyTypeSerializer<T> extends TypeSerializer<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> typeClass;

    private static final IFn freeze;
    private static final IFn thaw;

    static {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("taoensso.nippy"));
        freeze = Clojure.var("taoensso.nippy", "freeze");
        thaw = Clojure.var("taoensso.nippy", "thaw");
    }

    public NippyTypeSerializer(Class<T> typeClass) {
        this.typeClass = typeClass;
    }

    @Override
    public boolean isImmutableType() {
        return true; // Clojure persistent data structures are immutable
    }

    @Override
    public TypeSerializer<T> duplicate() {
        return this; // Stateless, can be shared
    }

    @Override
    public T createInstance() {
        return null; // Not applicable for Clojure types
    }

    @Override
    @SuppressWarnings("unchecked")
    public T copy(T from) {
        return from; // Immutable, no need to copy
    }

    @Override
    public T copy(T from, T reuse) {
        return from; // Immutable, no need to copy
    }

    @Override
    public int getLength() {
        return -1; // Variable length
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        byte[] bytes = (byte[]) freeze.invoke(record);
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        return (T) thaw.invoke(bytes);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int length = source.readInt();
        target.writeInt(length);
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        target.write(bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof NippyTypeSerializer) {
            NippyTypeSerializer<?> other = (NippyTypeSerializer<?>) obj;
            return typeClass.equals(other.typeClass);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return typeClass.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new NippyTypeSerializerSnapshot<>(typeClass);
    }

    /**
     * Snapshot for serializer compatibility checks during job restore.
     */
    public static class NippyTypeSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

        private Class<T> typeClass;

        public NippyTypeSerializerSnapshot() {
            // For deserialization
        }

        public NippyTypeSerializerSnapshot(Class<T> typeClass) {
            this.typeClass = typeClass;
        }

        @Override
        public int getCurrentVersion() {
            return 1;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeUTF(typeClass.getName());
        }

        @Override
        @SuppressWarnings("unchecked")
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
            String className = in.readUTF();
            try {
                this.typeClass = (Class<T>) Class.forName(className, true, userCodeClassLoader);
            } catch (ClassNotFoundException e) {
                throw new IOException("Could not load class: " + className, e);
            }
        }

        @Override
        public TypeSerializer<T> restoreSerializer() {
            return new NippyTypeSerializer<>(typeClass);
        }

        @Override
        public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
                TypeSerializerSnapshot<T> oldSerializerSnapshot) {
            if (oldSerializerSnapshot instanceof NippyTypeSerializerSnapshot) {
                NippyTypeSerializerSnapshot<T> oldSnapshot = (NippyTypeSerializerSnapshot<T>) oldSerializerSnapshot;
                if (typeClass.equals(oldSnapshot.typeClass)) {
                    return TypeSerializerSchemaCompatibility.compatibleAsIs();
                }
            }
            return TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
