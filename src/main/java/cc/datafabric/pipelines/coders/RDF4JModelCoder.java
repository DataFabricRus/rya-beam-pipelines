package cc.datafabric.pipelines.coders;

import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import org.apache.avro.util.ByteBufferOutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.VarInt;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

public class RDF4JModelCoder extends AtomicCoder<Model> {

    private static final RDFFormat RDF_FORMAT = RDFFormat.BINARY;
    private static final Coder<Model> INSTANCE = new RDF4JModelCoder();

    private RDF4JModelCoder() {
    }

    public static Coder<Model> of() {
        return INSTANCE;
    }

    @Override
    public void encode(Model value, OutputStream outStream) throws IOException {
        final ByteBufferOutputStream bos = new ByteBufferOutputStream();

        Rio.write(value, bos, RDF_FORMAT);

        final List<ByteBuffer> bufList = bos.getBufferList();
        long length = bufList.stream().reduce(0L, (l, bb) -> l + bb.limit(), (l1, l2) -> l1 + l2);
        VarInt.encode(length, outStream);

        for (ByteBuffer buf : bufList) {
            outStream.write(buf.array(), 0, buf.limit());
        }

    }

    @Override
    public Model decode(InputStream inStream) throws CoderException, IOException {
        /*
         *  Read the size of the value in the stream. To make sure that we don't read any byte of another value.
         */
        long length = VarInt.decodeLong(inStream);
        if (length < 0) {
            throw new IOException("Invalid length " + length);
        }

        InputStream limited = new CloseShieldInputStream(ByteStreams.limit(inStream, length));

        return Rio.parse(limited, "", RDF_FORMAT);
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(Model value) {
        return false;
    }

    @Override
    protected long getEncodedElementByteSize(Model value) throws Exception {
        try (CountingOutputStream cos = new CountingOutputStream(ByteStreams.nullOutputStream())) {
            Rio.write(value, cos, RDF_FORMAT);

            long count = cos.getCount();

            return (VarInt.getLength(count) + count);
        }
    }
}
