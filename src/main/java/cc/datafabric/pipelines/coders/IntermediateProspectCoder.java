package cc.datafabric.pipelines.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.rya.prospector.domain.IntermediateProspect;

import java.io.IOException;
import java.io.OutputStream;

import static com.google.common.base.Preconditions.checkArgument;

public class IntermediateProspectCoder extends WritableCoder<IntermediateProspect> {

    public static Coder<IntermediateProspect> of() {
        return new IntermediateProspectCoder();
    }

    private IntermediateProspectCoder() {
        super(IntermediateProspect.class);
    }

    @Override
    public void encode(IntermediateProspect value, OutputStream outStream) throws IOException {
        checkArgument(value != null, "Can't encode null prospect!");

        super.encode(value, outStream);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        super.verifyDeterministic();
    }
}
