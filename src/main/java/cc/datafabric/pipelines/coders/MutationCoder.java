package cc.datafabric.pipelines.coders;

import org.apache.accumulo.core.data.Mutation;
import org.apache.beam.sdk.io.hadoop.WritableCoder;

import java.io.IOException;
import java.io.OutputStream;

import static com.google.common.base.Preconditions.checkArgument;

public class MutationCoder extends WritableCoder<Mutation> {

    public MutationCoder() {
        super(Mutation.class);
    }

    @Override
    public void encode(Mutation value, OutputStream outStream) throws IOException {
        checkArgument(value != null, "Mutation can't be null!");

        super.encode(value, outStream);
    }
}
