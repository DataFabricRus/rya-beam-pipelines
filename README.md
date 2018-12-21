# Apache Beam Pipelines for Apache Rya

Pipelines:

* `bulkload` - loads RDF in the triplestore,
* `statistics` - reads triples from the SPO index, generates statistics (aka [Prospects Table](https://github.com/apache/incubator-rya/blob/master/extras/rya.manual/src/site/markdown/eval.md)) about the triples and writes them to a separate index.
* `elasticsearch` - reads triples from the SPO index, generates the full text index and writes it in Elasticsearch.

> At the moment, only the [DataFabric's fork](http://github.com/DataFabricRus/incubator-rya) of Apache Rya is supported.

## Supported runners

Current implementations were tested with [Google Dataflow](https://cloud.google.com/dataflow/) only.