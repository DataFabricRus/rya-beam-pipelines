package cc.datafabric.pipelines;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;

public class AvroContainer {

    private byte[] bytes;

    public AvroContainer pack(Object obj, Coder coder) throws CoderException {
        bytes = CoderUtils.encodeToByteArray(coder, obj);

        return this;
    }

    public Object unpack(Coder coder) throws CoderException {
        return CoderUtils.decodeFromByteArray(coder, bytes);
    }
}
