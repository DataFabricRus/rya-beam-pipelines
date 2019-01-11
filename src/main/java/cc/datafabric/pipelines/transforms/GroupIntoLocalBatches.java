package cc.datafabric.pipelines.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class GroupIntoLocalBatches<T> extends PTransform<PCollection<T>, PCollection<Iterable<T>>> {

    private static final Logger LOG = LoggerFactory.getLogger(GroupIntoLocalBatches.class);

    private final long batchSize;

    public static <T> GroupIntoLocalBatches<T> of(long batchSize) {
        return new GroupIntoLocalBatches<T>(batchSize);
    }

    private GroupIntoLocalBatches(long batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public PCollection<Iterable<T>> expand(PCollection<T> input) {
        return input.apply(ParDo.of(new DoFn<T, Iterable<T>>() {

            private transient List<T> batch = new ArrayList<>();

            @DoFn.StartBundle
            public void startBundle() {
                LOG.debug("Start up batch");
                batch = new ArrayList<>();
            }

            @DoFn.ProcessElement
            public void processElement(@Element T element, OutputReceiver<Iterable<T>> receiver) {
                checkArgument(element != null, "Can't batch nulls!");

                batch.add(element);

                if (batch.size() >= batchSize) {
                    LOG.debug("Flush batch on the threshold");

                    List<T> outputBatch = batch;
                    batch = new ArrayList<>();

                    receiver.outputWithTimestamp(outputBatch, Instant.now());
                }
            }

            @FinishBundle
            public void finishBundle(FinishBundleContext ctx) {
                LOG.debug("Flush batch on finished bundle");

                List<T> outputBatch = batch;
                batch = new ArrayList<>();

                ctx.output(outputBatch, Instant.now(), GlobalWindow.INSTANCE);
            }

        }));
    }
}
