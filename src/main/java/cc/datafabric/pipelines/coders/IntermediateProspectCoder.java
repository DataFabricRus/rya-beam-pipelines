package cc.datafabric.pipelines.coders;

import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.rya.prospector.domain.IntermediateProspect;

public class IntermediateProspectCoder extends WritableCoder<IntermediateProspect> {
    public IntermediateProspectCoder() {
        super(IntermediateProspect.class);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        super.verifyDeterministic();
    }
}
