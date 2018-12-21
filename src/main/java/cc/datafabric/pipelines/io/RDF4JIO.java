package cc.datafabric.pipelines.io;

import cc.datafabric.pipelines.GroupIntoLocalBatches;
import cc.datafabric.pipelines.coders.RDF4JModelCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Pattern;

public class RDF4JIO {

    private static final Logger LOG = LoggerFactory.getLogger(RDF4JIO.class);

    public static class Read extends PTransform<PCollection<String>, PCollection<Model>> {

        private final long bufferSize;

        public Read(long bufferSize) {
            this.bufferSize = bufferSize;
        }

        @Override
        public PCollection<Model> expand(PCollection<String> input) {
            PCollectionTuple separated = input
                    .apply(new Regex.Split(Pattern.compile("\\s*,\\s*"), false))
                    .apply(ParDo.of(new SeparateLineAndFileBasedFormats())
                            .withOutputTags(SeparateLineAndFileBasedFormats.LINE_BASED,
                                    TupleTagList.of(SeparateLineAndFileBasedFormats.FILE_BASED))
                    );

            PCollection<Model> lineBased = separated.get(SeparateLineAndFileBasedFormats.LINE_BASED)
                    .setCoder(StringUtf8Coder.of())
                    .apply(TextIO.readAll())
                    .apply(GroupIntoLocalBatches.of(this.bufferSize))
                    .apply(ParDo.of(new StringsToModel()))
                    .setCoder(RDF4JModelCoder.of());

            PCollection<Model> fileBased = separated.get(SeparateLineAndFileBasedFormats.FILE_BASED)
                    .setCoder(StringUtf8Coder.of())
                    .apply(FileIO.matchAll())
                    .apply(FileIO.readMatches())
                    .apply(ParDo.of(new FileToModel()))
                    .setCoder(RDF4JModelCoder.of());

            return PCollectionList.of(lineBased).and(fileBased).apply(Flatten.pCollections());
        }

        private static class SeparateLineAndFileBasedFormats extends DoFn<String, String> {

            public static final TupleTag<String> LINE_BASED = new TupleTag<>();
            public static final TupleTag<String> FILE_BASED = new TupleTag<>();

            @ProcessElement
            public void processElement(@Element String filePattern, MultiOutputReceiver mor) {
                Optional<RDFFormat> format = Rio.getParserFormatForFileName(filePattern);

                if (format.isPresent()) {
                    RDFFormat f = format.get();
                    if (f.equals(RDFFormat.NTRIPLES)) {
                        mor.get(LINE_BASED).output(filePattern);
                    } else {
                        mor.get(FILE_BASED).output(filePattern);
                    }
                } else {
                    LOG.warn("File pattern [{}] couldn't be matched with any RDF format! Skipping it.", filePattern);
                }
            }

        }

        private class FileToModel extends DoFn<FileIO.ReadableFile, Model> {

            @ProcessElement
            public void processElement(@Element FileIO.ReadableFile file, OutputReceiver<Model> out) {
                Optional<RDFFormat> format = Rio.getParserFormatForFileName(
                        file.getMetadata().resourceId().getFilename());

                if (format.isPresent()) {
                    try (ByteArrayInputStream bai = new ByteArrayInputStream(file.readFullyAsBytes())) {
                        Model model = Rio.parse(bai, "", format.get());

                        out.output(model);
                    } catch (IOException e) {
                        LOG.error("Failed to read file [{}]!", file.getMetadata().resourceId().toString());
                    }
                } else {
                    LOG.warn("File [{}] couldn't be matched with any RDF format! Skipping it.",
                            file.getMetadata().resourceId().toString());
                }
            }

        }

        private class StringsToModel extends DoFn<Iterable<String>, Model> {

            @ProcessElement
            public void processElement(ProcessContext ctx) throws IOException {
                StringBuilder sb = new StringBuilder();

                ctx.element().forEach(line -> sb.append(line).append("\n"));

                try (StringReader sr = new StringReader(sb.toString())) {
                    Model model = Rio.parse(sr, "", RDFFormat.NTRIPLES);

                    ctx.output(model);
                }
            }

        }
    }
}
