package cc.datafabric.pipelines.io;

import cc.datafabric.pipelines.coders.RangeCoder;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.io.Text;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@DoFn.BoundedPerElement
public class AccumuloSplittableDoFn extends DoFn<String, Map.Entry<Key, Value>> {

    private static final Logger LOG = LoggerFactory.getLogger(AccumuloSplittableDoFn.class);

    /**
     * In milliseconds.
     */
    private static final long WATERMARK_LAG = 5000;

    private final String instanceName;
    private final String zookeepers;
    private final String username;
    private final String password;
    private final Authorizations authorizations = Authorizations.EMPTY;
    private final boolean outputWithCurrentTimestamp;

    private transient Connector connector;

    AccumuloSplittableDoFn(String instanceName, String zookeepers, String username,
                           String password, boolean outputWithCurrentTimestamp) {
        this.instanceName = instanceName;
        this.zookeepers = zookeepers;
        this.username = username;
        this.password = password;
        this.outputWithCurrentTimestamp = outputWithCurrentTimestamp;
    }

    @Setup
    public void setup() throws Exception {
        final Instance instance = new ZooKeeperInstance(instanceName, zookeepers);

        this.connector = instance.getConnector(username, new PasswordToken(password));
    }

    @ProcessElement
    public ProcessContinuation processElement(ProcessContext ctx, AccumuloRangeTracker tracker)
            throws TableNotFoundException {
        final String tableName = ctx.element();

        try (Scanner scanner = connector.createScanner(tableName, authorizations)) {
            scanner.setRange(tracker.currentRestriction());

            for (Map.Entry<Key, Value> value : scanner) {
                final Key key = value.getKey();
                if (!tracker.tryClaim(key)) {
                    return ProcessContinuation.stop();
                }

                if (outputWithCurrentTimestamp) {
                    ctx.outputWithTimestamp(value, Instant.now());
                } else {
                    ctx.output(value);
                }
            }

            if (outputWithCurrentTimestamp) {
                ctx.updateWatermark(Instant.now().minus(WATERMARK_LAG));
            }

            tracker.markDone();

            return ProcessContinuation.stop();
        }
    }

    @GetRestrictionCoder
    public Coder<Range> getRestrictionCoder() {
        return RangeCoder.of();
    }

    @GetInitialRestriction
    public Range getInitialRestriction(String tableName) {
        return new Range();
    }

    @SplitRestriction
    public void splitRestriction(String tableName, Range initialRange, OutputReceiver<Range> receiver)
            throws Exception {
        if (!initialRange.isInfiniteStartKey() && !initialRange.isInfiniteStopKey()) {
            /*
             * Only initial range may be split.
             */
            receiver.output(new Range(initialRange));
        }

        List<Text> splits = Arrays.asList(connector.tableOperations()
                .listSplits(tableName)
                .toArray(new Text[0]));

        if (splits.isEmpty()) {
            // Then we're going to read the whole table at once
            LOG.info("Table {}. There is no splits!", tableName);

            receiver.output(new Range());
        } else {
            LOG.info("Table {}. Found {} splits", tableName, splits.size());
            int index = 0;
            while (index < splits.size()) {
                Range range;
                if (index == 0) {
                    range = new Range(null, false, splits.get(index), false);
                } else {
                    range = new Range(splits.get(index - 1), true, splits.get(index), false);
                }

                receiver.output(range);

                index++;
            }

            receiver.output(new Range(splits.get(index - 1), true, null, false));
        }
    }

    @NewTracker
    public AccumuloRangeTracker newTracker(Range range) {
        return new AccumuloRangeTracker(range);
    }

}
