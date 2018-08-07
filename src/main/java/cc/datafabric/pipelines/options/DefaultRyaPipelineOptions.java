package cc.datafabric.pipelines.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public interface DefaultRyaPipelineOptions extends DataflowPipelineOptions {

    String getAccumuloName();

    void setAccumuloName(String name);

    String getAccumuloUsername();

    void setAccumuloUsername(String username);

    String getAccumuloPassword();

    void setAccumuloPassword(String password);

    String getZookeeperServers();

    void setZookeeperServers(String servers);

}
