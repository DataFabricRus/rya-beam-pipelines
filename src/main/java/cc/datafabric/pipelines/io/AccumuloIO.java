package cc.datafabric.pipelines.io;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import java.util.HashMap;
import java.util.Map;

public class AccumuloIO {

    public static class Read extends PTransform<PCollection<String>, PCollection<Map.Entry<Key, Value>>> {

        private final String instanceName;
        private final String zookeepers;
        private final String username;
        private final String password;
        private int iteratorPriority;
        private Class<? extends SortedKeyValueIterator<Key, Value>> iteratorClass;
        private Map<String, String> iteratorOptions;

        public Read(String instanceName, String zookeepers, String username, String password) {
            this.instanceName = instanceName;
            this.zookeepers = zookeepers;
            this.username = username;
            this.password = password;
        }

        public AccumuloIO.Read withScanIterator(
                int priority, Class<? extends SortedKeyValueIterator<Key, Value>> iteratorClass,
                String optionName, String[] optionValue) {
            Preconditions.checkArgument(optionName != null && optionValue != null);

            this.iteratorPriority = priority;
            this.iteratorClass = iteratorClass;

            Map<String, String> options = new HashMap<>();
            options.put(optionName, String.join(",", optionValue));

            this.iteratorOptions = options;

            return this;
        }

        @Override
        public PCollection<Map.Entry<Key, Value>> expand(PCollection<String> input) {
            return input
                    .apply(new AccumuloReadAll(instanceName, zookeepers, username, password,
                            iteratorPriority, iteratorClass, iteratorOptions));
        }
    }

}
