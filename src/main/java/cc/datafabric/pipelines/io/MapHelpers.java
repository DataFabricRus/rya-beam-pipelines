package cc.datafabric.pipelines.io;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

public class MapHelpers {

    public static Map<String, String> singletonMap(String optionName, String[] optionValue) {
        Preconditions.checkArgument(optionName != null && optionValue != null);

        Map<String, String> options = new HashMap<>();
        options.put(optionName, String.join(",", optionValue));

        return options;
    }

}
