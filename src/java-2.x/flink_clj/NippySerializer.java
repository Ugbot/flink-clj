package flink_clj;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import clojure.java.api.Clojure;
import clojure.lang.IFn;

/**
 * Kryo serializer that uses Nippy for fast serialization of Clojure data structures.
 *
 * Nippy provides significantly faster serialization than default Kryo for
 * Clojure's persistent data structures (maps, vectors, sets, etc.).
 *
 * Note: Flink 2.x uses Kryo 5.x which has a different method signature for read().
 */
public class NippySerializer<T> extends Serializer<T> {

    private static final IFn freeze;
    private static final IFn thaw;

    static {
        // Load Nippy namespace
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("taoensso.nippy"));

        // Get freeze/thaw functions
        freeze = Clojure.var("taoensso.nippy", "freeze");
        thaw = Clojure.var("taoensso.nippy", "thaw");
    }

    @Override
    public void write(Kryo kryo, Output output, T object) {
        byte[] bytes = (byte[]) freeze.invoke(object);
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T read(Kryo kryo, Input input, Class<? extends T> type) {
        int length = input.readInt();
        byte[] bytes = input.readBytes(length);
        return (T) thaw.invoke(bytes);
    }
}
