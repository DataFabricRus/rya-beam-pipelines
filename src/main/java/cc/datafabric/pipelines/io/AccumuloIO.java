package cc.datafabric.pipelines.io;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Map;

public class AccumuloIO {

    public static class Read extends PTransform<PCollection<String>, PCollection<Map.Entry<Key, Value>>> {

        private final String instanceName;
        private final String zookeepers;
        private final String username;
        private final String password;

        public Read(String instanceName, String zookeepers, String username,
                    String password) {
            this.instanceName = instanceName;
            this.zookeepers = zookeepers;
            this.username = username;
            this.password = password;
        }

        @Override
        public PCollection<Map.Entry<Key, Value>> expand(PCollection<String> input) {
            return input.apply(
                    ParDo.of(new AccumuloSplittableDoFn(
                            instanceName, zookeepers, username, password, false))
            );
        }
    }

}
