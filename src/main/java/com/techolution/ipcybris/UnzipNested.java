
package com.techolution.ipcybris;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.Tuple;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.annotations.VisibleForTesting;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.*;

import static org.apache.beam.sdk.util.GcsUtil.*;

import com.google.pubsub.v1.ProjectTopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.Count;


/**
 * This pipeline unpacks(unzips) file(s) from Google Cloud Storage and re-uploads them to a destination
 * location.
 *
 * <p><b>Parameters</b>
 *
 * <p>The {@code --inputFilePattern} parameter specifies a file glob to process. Files found can be
 * expressed in the following formats:
 *
 * <pre>
 * --inputFilePattern=gs://bucket-name/compressed-dir/*
 * --inputFilePattern=gs://bucket-name/compressed-dir/demo*.gz
 * </pre>
 *
 * <p>The {@code --outputDirectory} parameter can be expressed in the following formats:
 *
 * <pre>
 * --outputDirectory=gs://bucket-name
 * --outputDirectory=gs://bucket-name/decompressed-dir
 * </pre>
 *
 * <p>The {@code --outputFailureFile} parameter indicates the file to write the names of the files
 * which failed decompression and their associated error messages. This file can then be used for
 * subsequent processing by another process outside of Dataflow (e.g. send an email with the
 * failures, etc.). If there are no failures, the file will still be created but will be empty. The
 * failure file structure contains both the file that caused the error and the error message in CSV
 * format. The file will contain one header row and two columns (Filename, Error). The filename
 * output to the failureFile will be the full path of the file for ease of debugging.
 *
 * <pre>
 * --outputFailureFile=gs://bucket-name/decompressed-dir/failed.csv
 * </pre>
 *
 * <p>Example Output File:
 *
 * <pre>
 * Filename,Error
 * gs://docs-demo/compressedFile.gz, File is malformed or not compressed in BZIP2 format.
 * </pre>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.BulkDecompressor \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
 * --tempLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
 * --runner=DataflowRunner \
 * --inputFilePattern=gs://${PROJECT_ID}/compressed-dir/*.gz \
 * --outputDirectory=gs://${PROJECT_ID}/decompressed-dir \
 * --outputFailureFile=gs://${PROJECT_ID}/decompressed-dir/failed.csv"
 * </pre>
 */
public class UnzipNested {

    /** The logger to output status messages to. */
//    private static final Logger LOG = LoggerFactory.getLogger(UnzipNested.class);

    /**
     * A list of the {@link Compression} values excluding {@link Compression#AUTO} and {@link
     * Compression#UNCOMPRESSED}.
     */
    @VisibleForTesting
    static final Set<Compression> SUPPORTED_COMPRESSIONS =
            Stream.of(Compression.values())
                    .filter(value -> value != Compression.AUTO && value != Compression.UNCOMPRESSED)
                    .collect(Collectors.toSet());

    /**
     * The error msg given when the pipeline matches a file but cannot determine the compression.
     */
    @VisibleForTesting
    static final String UNCOMPRESSED_ERROR_MSG =
            "Skipping file %s because it did not match any compression mode (%s)";

    @VisibleForTesting
    static final String MALFORMED_ERROR_MSG =
            "The file resource %s is malformed or not in %s compressed format.";

    @VisibleForTesting
    static final TupleTag<String> DECOMPRESS_MAIN_OUT_TAG = new TupleTag<String>() {
    };

    @VisibleForTesting
    static final TupleTag<KV<String, String>> DEADLETTER_TAG = new TupleTag<KV<String, String>>() {
    };





    /**
     * The {@link Options} class provides the custom execution options passed by the executor at the
     * command-line.
     */
    public interface Options extends PipelineOptions {
        @Description("The input file pattern to read from (e.g. gs://bucket-name/compressed/*.gz)")
        @Required
        ValueProvider<String> getInputFilePattern();

        void setInputFilePattern(ValueProvider<String> value);

        @Description("The output location to write to (e.g. gs://bucket-name/decompressed)")
        @Required
        ValueProvider<String> getOutputDirectory();

        void setOutputDirectory(ValueProvider<String> value);

        @Description("The name of the topic which data should be published to. "
                + "The name should be in the format of projects/<project-id>/topics/<topic-name>.")
        @Required
        ValueProvider<String> getOutputTopic();

        void setOutputTopic(ValueProvider<String> value);


        @Description("The name of the topic which data should be published to. "
                + "The name should be in the format of projects/<project-id>/topics/<topic-name>.")
        @Required
        ValueProvider<String> getErrorTopic();

        void setErrorTopic(ValueProvider<String> value);
    }


    public static void main(String[] args) {

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        run(options);
    }

    /**
     * Runs the pipeline to completion with the specified options. This method does not wait until the
     * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
     * object to block until the pipeline is finished running if blocking programmatic execution is
     * required.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(Options options) {

        /*
         * Steps:
         *   1) Decompress the input files found and output them to the output directory
         */
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        // Run the pipeline over the work items.

        final TupleTag<String> successTag = new TupleTag<String>() {
        };
        final TupleTag<String> errorTag = new TupleTag<String>() {
        };

        PCollection<MatchResult.Metadata> match_patterns = pipeline.apply("MatchFile(s)", FileIO.match().filepattern(options.getInputFilePattern()));

        PCollectionTuple pubsub_messages = match_patterns.apply("DecompressFile(s)", ParDo.of(new DecompressNew(options.getOutputDirectory(), successTag, errorTag)).withOutputTags(successTag, TupleTagList.of(errorTag)));

        pubsub_messages.get(successTag).apply("Write Success to PubSub", PubsubIO.writeStrings().to(options.getOutputTopic()));

        pubsub_messages.get(errorTag).apply("Write Failures to PubSub", PubsubIO.writeStrings().to(options.getErrorTopic()));


        return pipeline.run();
    }

    /**
     * Performs the decompression of an object on Google Cloud Storage and uploads the decompressed
     * object back to a specified destination location.
     */
    @SuppressWarnings("serial")
    public static class DecompressNew extends DoFn<MatchResult.Metadata, String> {
        private static final long serialVersionUID = 2015166770614756341L;
        private final TupleTag<String> successTag;
        private final TupleTag<String> errorTag;
        private long filesUnzipped = 0;
        private String outp = "NA";
        private List<String> publishresults = new ArrayList<>();
        private static final Logger log = LoggerFactory.getLogger(UnzipNested.class);
        private List<String> images = new ArrayList<>();
        private final ValueProvider<String> destinationLocation;

        DecompressNew(ValueProvider<String> destinationLocation, TupleTag<String> successTag, TupleTag<String> errorTag) {
            this.destinationLocation = destinationLocation;
            this.successTag = successTag;
            this.errorTag = errorTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            ResourceId p = c.element().resourceId();
            GcsUtilFactory factory = new GcsUtilFactory();
            GcsUtil u = factory.create(c.getPipelineOptions());
            byte[] buffer = new byte[100000000];
            try {
                SeekableByteChannel sek = u.open(GcsPath.fromUri(p.toString()));
                InputStream is;
                is = Channels.newInputStream(sek);
                BufferedInputStream bis = new BufferedInputStream(is);
                ZipInputStream zis = new ZipInputStream(bis);
                ZipEntry ze = zis.getNextEntry();
                while (ze != null) {
                    String entry_name = ze.getName();
                    if (entry_name.toUpperCase().contains(".TIF")) {
                        try {
                            log.info("extracting :" + entry_name);
                            WritableByteChannel wri = u.create(GcsPath.fromUri(this.destinationLocation.get() + entry_name), getType(entry_name));
                            OutputStream os = Channels.newOutputStream(wri);
                            int len;
                            while ((len = zis.read(buffer)) > 0) {
                                os.write(buffer, 0, len);
                            }
                            os.close();
                            log.info("extraction success : " + entry_name);
                            publishresults.add(this.destinationLocation.get() + entry_name);
                        } catch (Exception e) {
                            log.error(e.getMessage());
                            String error_message = e.getMessage();
                            c.output(errorTag, error_message);
                        }
                    }
                    ze = zis.getNextEntry();
                }
                outp = getFinalOutput(publishresults);
                publishresults.clear();
                images.clear();
                zis.closeEntry();
                zis.close();
                filesUnzipped++;
                log.info("unzipped count" + filesUnzipped);
            } catch (Exception e) {
                log.error("error encountered" + e);
            }
            c.output(successTag, outp);
        }

        private String getFinalOutput(List<String> publishresults) {
            Gson gsonBuilder = new GsonBuilder().create();
            JsonParser jsonParser = new JsonParser();
            for (String path : publishresults) {
                if (path.toUpperCase().contains(".TIF")) {
                    images.add(path);
                }
            }
            JsonObject pubsubout = new JsonObject();
            JsonArray imagesArray = new JsonArray();
            if (!images.isEmpty()) {
                imagesArray = jsonParser.parse(gsonBuilder.toJson(images)).getAsJsonArray();
            }
            pubsubout.add("images", imagesArray);
            return pubsubout.toString();
        }

        private String getType(String fName) {
            if (fName.endsWith(".zip")) {
                return "application/x-zip-compressed";
            } else if (fName.endsWith(".tar")) {
                return "application/x-tar";
            } else if (fName.toLowerCase().endsWith(".tif")) {
                return "image/tiff";
            } else {
                return "text/plain";
            }
        }
    }
}
