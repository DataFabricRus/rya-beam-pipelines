package cc.datafabric.pipelines.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapEntryCoder<K, V> extends StructuredCoder<Map.Entry<K, V>> {

    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;

    public static <K, V> Coder<Map.Entry<K, V>> of(Coder<K> keyCoder, Coder<V> valueCoder) {
        return new MapEntryCoder<>(keyCoder, valueCoder);
    }

    private MapEntryCoder(Coder<K> keyCoder, Coder<V> valueCoder) {
        this.keyCoder = keyCoder;
        this.valueCoder = valueCoder;
    }

    @Override
    public void encode(Map.Entry<K, V> value, OutputStream outStream) throws IOException {
        keyCoder.encode(value.getKey(), outStream);
        valueCoder.encode(value.getValue(), outStream);
    }

    @Override
    public Map.Entry<K, V> decode(InputStream inStream) throws IOException {
        K key = keyCoder.decode(inStream);
        V value = valueCoder.decode(inStream);

        return new HashMap.SimpleEntry<>(key, value);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Arrays.asList(keyCoder, valueCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        keyCoder.verifyDeterministic();
        valueCoder.verifyDeterministic();
    }

    @Override
    public boolean consistentWithEquals() {
        return keyCoder.consistentWithEquals() && valueCoder.consistentWithEquals();
    }

    @Override
    public TypeDescriptor<Map.Entry<K, V>> getEncodedTypeDescriptor() {
        return new TypeDescriptor<Map.Entry<K, V>>() {
        }.where(
                new TypeParameter<K>() {
                }, keyCoder.getEncodedTypeDescriptor())
                .where(new TypeParameter<V>() {
                }, valueCoder.getEncodedTypeDescriptor());
    }
}
