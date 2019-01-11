package cc.datafabric.pipelines;

import com.google.common.primitives.Bytes;
import org.apache.accumulo.core.data.Key;

import java.nio.charset.StandardCharsets;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTE;

public abstract class RyaSchemaUtils {

    public static String extractSubject(Key key) {
        try {
            byte[] row = key.getRow().getBytes();
            int indexTo = Bytes.indexOf(row, DELIM_BYTE);

            return new String(row, 0, indexTo, StandardCharsets.UTF_8);
        } catch (IndexOutOfBoundsException __) {
            return null;
        }
    }

}
