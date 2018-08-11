package cc.datafabric.pipelines.io;

import cc.datafabric.pipelines.coders.RangeCoder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.*;

@DefaultCoder(AccumuloRangeTracker.AccumuloRangeTrackerCoder.class)
public class AccumuloRangeTracker extends RestrictionTracker<Range, Key> {

    private Range range;
    @Nullable
    private Key lastClaimedKey = null;
    @Nullable
    private Key lastAttemptedKey = null;

    AccumuloRangeTracker(Range range) {
        this(range, null, null);
    }

    private AccumuloRangeTracker(Range range, Key lastAttemptedKey, Key lastClaimedKey) {
        this.range = checkNotNull(range);
        this.lastAttemptedKey = lastAttemptedKey;
        this.lastClaimedKey = lastClaimedKey;
    }

    @Override
    public Range currentRestriction() {
        return new Range(range);
    }

    @Override
    public Range checkpoint() {
        checkState(lastClaimedKey != null, "Can't checkpoint before any key was successfully claimed");
        Range res = new Range(new Key(lastClaimedKey), false,
                range.isInfiniteStopKey() ? null : new Key(range.getEndKey()), range.isEndKeyInclusive());
        this.range = new Range(range.isInfiniteStartKey() ? null : range.getStartKey(), range.isStartKeyInclusive(),
                new Key(lastClaimedKey), true);
        return res;
    }

    /**
     * Attempts to claim the given key.
     *
     * <p>Must be larger than the last successfully claimed key.
     *
     * @return {@code true} if the key was successfully claimed, {@code false} if it is outside the
     * current {@link Range} of this tracker (in that case this operation is a no-op).
     */
    @Override
    protected synchronized boolean tryClaimImpl(Key key) {
        checkArgument(
                lastAttemptedKey == null || key.compareTo(lastAttemptedKey) > 0,
                "Trying to claim key %s while last attempted was %s",
                key,
                lastAttemptedKey);
        checkArgument(
                range.isInfiniteStartKey() || key.compareTo(range.getStartKey()) > -1,
                "Trying to claim key %s before start of the range %s",
                key,
                range);
        lastAttemptedKey = key;
        // No respective checkArgument for i < range.to() - it's ok to try claiming keys beyond
        if (!range.isInfiniteStopKey() && key.compareTo(range.getEndKey()) > -1) {
            return false;
        }
        lastClaimedKey = key;
        return true;
    }

    /**
     * Marks that there are no more keys to be claimed in the range.
     *
     * <p>E.g., a {@link DoFn} reading a file and claiming the key of each record in the file might
     * call this if it hits EOF - even though the last attempted claim was before the end of the
     * range, there are no more keys to claim.
     */
    public synchronized void markDone() {
        if (!range.isInfiniteStopKey()) {
            lastAttemptedKey = range.getEndKey();
        }
    }

    @Override
    public synchronized void checkDone() throws IllegalStateException {
        checkState(lastAttemptedKey != null, "Can't check if done before any key claim was attempted");
        Key nextKey = next(lastAttemptedKey);
        checkState(
                range.isInfiniteStopKey() || nextKey.compareTo(range.getEndKey()) > -1,
                "Last attempted key was %s in range %s, claiming work in [%s, %s) was not attempted",
                lastAttemptedKey,
                range,
                nextKey,
                range.getEndKey());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("range", range)
                .add("lastClaimedKey", lastClaimedKey)
                .add("lastAttemptedKey", lastAttemptedKey)
                .toString();
    }

    public static class AccumuloRangeTrackerCoder extends StructuredCoder<AccumuloRangeTracker> {

        private final Coder<Range> rangeCoder = RangeCoder.of();
        private final Coder<Key> keyCoder = WritableCoder.of(Key.class);

        @Override
        public void encode(AccumuloRangeTracker value, OutputStream outStream) throws CoderException, IOException {
            rangeCoder.encode(value.range, outStream);
            keyCoder.encode(value.lastAttemptedKey, outStream);
            keyCoder.encode(value.lastClaimedKey, outStream);
        }

        @Override
        public AccumuloRangeTracker decode(InputStream inStream) throws CoderException, IOException {
            Range range = rangeCoder.decode(inStream);
            Key lastAttemptedKey = keyCoder.decode(inStream);
            Key lastClaimedKey = keyCoder.decode(inStream);

            return new AccumuloRangeTracker(range, lastAttemptedKey, lastClaimedKey);
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Arrays.asList(rangeCoder, keyCoder);
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
            rangeCoder.verifyDeterministic();
            keyCoder.verifyDeterministic();
        }
    }

    // Utility methods

    /**
     * Calculates the next {@link Key} for a given key by incrementing by one using byte
     * arithmetic. If the input key is empty it assumes it is a lower bound and returns the 00 byte
     * array.
     */
    @VisibleForTesting
    private static Key next(Key key) {
        return key.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS);
    }
}
