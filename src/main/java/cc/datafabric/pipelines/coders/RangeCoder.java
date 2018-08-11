package cc.datafabric.pipelines.coders;

import org.apache.accumulo.core.data.Range;
import org.apache.beam.sdk.io.hadoop.WritableCoder;

public class RangeCoder extends WritableCoder<Range> {
    private RangeCoder() {
        super(Range.class);
    }

    public static RangeCoder of() {
        return new RangeCoder();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
//        super.verifyDeterministic();
    }
}
