package com.epam.bdcc.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.numenta.nupic.network.Network;
import org.numenta.nupic.serialize.HTMObjectInput;
import org.numenta.nupic.serialize.HTMObjectOutput;
import org.numenta.nupic.serialize.SerialConfig;
import org.numenta.nupic.serialize.SerializerCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

// SerDe HTM objects using SerializerCore (https://github.com/RuedigerMoeller/fast-serialization)
public class SparkKryoHTMSerializer<T> extends Serializer<T> {
    private static final Logger EPAM_LOGGER = LoggerFactory.getLogger("com.epam");
    private final SerializerCore htmSerializer = new SerializerCore(SerialConfig.DEFAULT_REGISTERED_TYPES);

    public SparkKryoHTMSerializer() {
        htmSerializer.registerClass(Network.class);
    }

    @Override
    //we don't need to copy something, so nothing to do
    public T copy(Kryo kryo, T original) {
        //TODO : Add implementation for copy, if needed
        throw new UnsupportedOperationException("Add implementation for copy");
    }

    @Override
    //serialize object
    public void write(Kryo kryo, Output output, T t) {
        //       TODO : Add implementation for serialization
        try {
            try (ByteArrayOutputStream stream = new ByteArrayOutputStream(4096)) {
                // write the object using the HTM serializer
                HTMObjectOutput writer = htmSerializer.getObjectOutput(stream);
                writer.writeObject(t, t.getClass());
                writer.close();

                // write the serialized data
                output.writeInt(stream.size());
                stream.writeTo(output);

                EPAM_LOGGER.debug("------------------- wrote {" + stream.size() + "} bytes, class " + t.getClass() + ", Thread: " + Thread.currentThread().getName());
            }
        } catch (IOException e) {
            throw new KryoException("problems writing object" + e);
        }
    }

    @Override
    //deserialize object
    public T read(Kryo kryo, Input input, Class<T> aClass) {

        //TODO : Add implementation for deserialization
        // read the serialized data
        byte[] data = new byte[input.readInt()];
        input.readBytes(data);

        try {
            try (ByteArrayInputStream stream = new ByteArrayInputStream(data)) {
                HTMObjectInput reader = htmSerializer.getObjectInput(stream);
                EPAM_LOGGER.debug("------------------- read and returned {" + data.length + "} bytes, class " + aClass + ", Thread: " + Thread.currentThread().getName());
                return (T) reader.readObject(aClass);
            }
        } catch (Exception e) {
            throw new KryoException("error reading object" + e);
        }
    }

    public static void registerSerializers(Kryo kryo) {
        kryo.register(Network.class, new SparkKryoHTMSerializer<>());
        for (Class c : SerialConfig.DEFAULT_REGISTERED_TYPES)
            kryo.register(c, new SparkKryoHTMSerializer<>());
    }
}
