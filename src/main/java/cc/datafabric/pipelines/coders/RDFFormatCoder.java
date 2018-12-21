package cc.datafabric.pipelines.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class RDFFormatCoder extends StructuredCoder<RDFFormat> {

    private static final List<RDFFormat> RDF_FORMATS = Arrays.asList(RDFFormat.NTRIPLES, RDFFormat.NQUADS);
    private static final RDFFormatCoder INSTANCE = new RDFFormatCoder();

    private final Coder<String> stringCoder = StringUtf8Coder.of();

    private RDFFormatCoder() {
    }

    public static final Coder<RDFFormat> of() {
        return INSTANCE;
    }

    @Override
    public void encode(RDFFormat value, OutputStream outStream) throws CoderException, IOException {
        stringCoder.encode(value.getName(), outStream);
    }

    @Override
    public RDFFormat decode(InputStream inStream) throws CoderException, IOException {
        String name = stringCoder.decode(inStream);
        Optional<RDFFormat> found = RDF_FORMATS.parallelStream()
                .filter(it -> it.getName().equalsIgnoreCase(name))
                .findFirst();

        if (found.isPresent()) {
            return found.get();
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {

    }
}
