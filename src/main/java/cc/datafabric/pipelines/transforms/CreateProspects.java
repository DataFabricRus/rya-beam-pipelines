package cc.datafabric.pipelines.transforms;

import cc.datafabric.pipelines.coders.MapEntryCoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.apache.rya.prospector.domain.IntermediateProspect;
import org.apache.rya.prospector.domain.TripleValueType;
import org.apache.rya.prospector.plans.IndexWorkPlan;
import org.apache.rya.prospector.utils.ProspectorConstants;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class CreateProspects {

    private static final Logger LOG = LoggerFactory.getLogger(CreateProspects.class);

    public static PTransform<PCollection<Map.Entry<Key, Value>>, PCollection<Map.Entry<IntermediateProspect, Long>>> fromAccumuloRow() {
        return new AccumuloRowToProspects();
    }

    public static PTransform<PCollection<Model>, PCollection<Map.Entry<IntermediateProspect, Long>>> fromModel() {
        return new ModelToProspects();
    }

    private static List<Map.Entry<IntermediateProspect, Long>> ryaStatementToIntermediateProspects(RyaStatement statement) {
        try {
            final String subject = statement.getSubject().getData();
            final String predicate = statement.getPredicate().getData();
            final String subjpred = statement.getSubject().getData() + IndexWorkPlan.DELIM
                    + statement.getPredicate().getData();
            final String predobj = statement.getPredicate().getData() + IndexWorkPlan.DELIM
                    + statement.getObject().getData();
            final String subjobj = statement.getSubject().getData() + IndexWorkPlan.DELIM
                    + statement.getObject().getData();
            final RyaType object = statement.getObject();
            final int localIndex = URIUtil.getLocalNameIndex(subject);
            final String namespace = subject.substring(0, localIndex - 1);
            final String visibility = new String(statement.getColumnVisibility(), StandardCharsets.UTF_8);

            final List<Map.Entry<IntermediateProspect, Long>> prospects = new ArrayList<>();

            prospects.add(
                    new HashMap.SimpleEntry<>(
                            IntermediateProspect.builder()
                                    .setIndex(ProspectorConstants.COUNT)
                                    .setData(subject)
                                    .setDataType(IndexWorkPlan.URITYPE)
                                    .setTripleValueType(TripleValueType.SUBJECT)
                                    .setVisibility(visibility)
                                    .build(),
                            1L
                    )
            );
            prospects.add(
                    new HashMap.SimpleEntry<>(
                            IntermediateProspect.builder()
                                    .setIndex(ProspectorConstants.COUNT)
                                    .setData(predicate)
                                    .setDataType(IndexWorkPlan.URITYPE)
                                    .setTripleValueType(TripleValueType.PREDICATE)
                                    .setVisibility(visibility)
                                    .build(),
                            1L
                    )
            );
            prospects.add(
                    new HashMap.SimpleEntry<>(
                            IntermediateProspect.builder()
                                    .setIndex(ProspectorConstants.COUNT)
                                    .setData(object.getData())
                                    .setDataType(object.getDataType().stringValue())
                                    .setTripleValueType(TripleValueType.OBJECT)
                                    .setVisibility(visibility)
                                    .build(),
                            1L
                    )
            );
            prospects.add(
                    new HashMap.SimpleEntry<>(
                            IntermediateProspect.builder()
                                    .setIndex(ProspectorConstants.COUNT)
                                    .setData(subjpred)
                                    .setDataType(XMLSchema.STRING.toString())
                                    .setTripleValueType(TripleValueType.SUBJECT_PREDICATE)
                                    .setVisibility(visibility)
                                    .build(),
                            1L
                    )
            );
            prospects.add(
                    new HashMap.SimpleEntry<>(
                            IntermediateProspect.builder()
                                    .setIndex(ProspectorConstants.COUNT)
                                    .setData(subjobj)
                                    .setDataType(XMLSchema.STRING.toString())
                                    .setTripleValueType(TripleValueType.SUBJECT_OBJECT)
                                    .setVisibility(visibility)
                                    .build(),
                            1L
                    )
            );
            prospects.add(
                    new HashMap.SimpleEntry<>(
                            IntermediateProspect.builder()
                                    .setIndex(ProspectorConstants.COUNT)
                                    .setData(predobj)
                                    .setDataType(XMLSchema.STRING.toString())
                                    .setTripleValueType(TripleValueType.PREDICATE_OBJECT)
                                    .setVisibility(visibility)
                                    .build(),
                            1L
                    )
            );
            prospects.add(
                    new HashMap.SimpleEntry<>(
                            IntermediateProspect.builder()
                                    .setIndex(ProspectorConstants.COUNT)
                                    .setData(namespace)
                                    .setDataType(IndexWorkPlan.URITYPE)
                                    .setTripleValueType(TripleValueType.ENTITY)
                                    .setVisibility(visibility)
                                    .build(),
                            1L
                    )
            );

            return prospects;
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);

            throw e;
        }
    }

    private static class AccumuloRowToProspects extends PTransform<PCollection<Map.Entry<Key, Value>>, PCollection<Map.Entry<IntermediateProspect, Long>>> {

        @Override
        public PCollection<Map.Entry<IntermediateProspect, Long>> expand(PCollection<Map.Entry<Key, Value>> input) {
            return input
                    .apply(ParDo.of(new DoFn<Map.Entry<Key, Value>, Map.Entry<IntermediateProspect, Long>>() {
                        @ProcessElement
                        public void processElement(ProcessContext ctx) throws TripleRowResolverException {
                            final Map.Entry<Key, Value> kv = ctx.element();

                            RyaTripleContext rtc = new RyaTripleContext(false);
                            RyaStatement statement = rtc
                                    .deserializeTriple(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO,
                                            new TripleRow(
                                                    kv.getKey().getRow().getBytes(),
                                                    kv.getKey().getColumnFamily().getBytes(),
                                                    kv.getKey().getColumnQualifier().getBytes(),
                                                    kv.getKey().getTimestamp(),
                                                    kv.getKey().getColumnVisibility().getBytes(),
                                                    kv.getValue().get()
                                            ));


                            ryaStatementToIntermediateProspects(statement).forEach(ctx::output);

                        }
                    }))
                    .setCoder(MapEntryCoder.of(WritableCoder.of(IntermediateProspect.class), VarLongCoder.of()));
        }
    }

    private static class ModelToProspects extends PTransform<PCollection<Model>, PCollection<Map.Entry<IntermediateProspect, Long>>> {

        @Override
        public PCollection<Map.Entry<IntermediateProspect, Long>> expand(PCollection<Model> input) {
            return input
                    .apply(ParDo.of(new DoFn<Model, Map.Entry<IntermediateProspect, Long>>() {

                        @ProcessElement
                        public void processElement(
                                @Element Model model,
                                OutputReceiver<Map.Entry<IntermediateProspect, Long>> receiver
                        ) {
                            model.stream()
                                    .map(it -> {
                                        RyaStatement statement = RdfToRyaConversions.convertStatement(it);
                                        statement.setColumnVisibility(new ColumnVisibility().getExpression());

                                        return statement;
                                    })
                                    .forEach(it -> ryaStatementToIntermediateProspects(it).forEach(receiver::output));
                        }
                    }))
                    .setCoder(MapEntryCoder.of(WritableCoder.of(IntermediateProspect.class), VarLongCoder.of()));
        }
    }
}
