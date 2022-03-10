package com.gcp.pipeline;

import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class PubSubTopicToBigQuery {
    /**
     * The logger to output status messages to.
     */
    private static final Logger LOG = LoggerFactory.getLogger(PubSubTopicToBigQuery.class);

    /**
     * The {@link Options} class provides the custom execution options passed by the
     * executor at the command-line.
     */
    public interface Options extends DataflowPipelineOptions {
        @Description("Window duration length, in seconds")
        Integer getWindowDuration();
        void setWindowDuration(Integer windowDuration);

        @Description("BigQuery aggregate table name")
        String getAggregateTableName();
        void setAggregateTableName(String aggregateTableName);

        @Description("Input topic name")
        String getInputTopic();
        void setInputTopic(String inputTopic);

        @Description("BigQuery raw table name")
        String getRawTableName();
        void setRawTableName(String rawTableName);

    }


    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        run(options);
    }


    /**
     * A DoFn acccepting Json and outputing CommonLog with Beam Schema
     */

    static class JsonToCommonLog extends DoFn<String, CommonLog> {

        @ProcessElement
        public void processElement(@Element String json, OutputReceiver<CommonLog> r) throws Exception {
            Gson gson = new Gson();
            CommonLog commonLog = gson.fromJson(json, CommonLog.class);
            r.output(commonLog);
        }
    }

    public static final Schema TableSchema = Schema
            .builder()
            .addInt64Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();


    public static PipelineResult run(Options options) {

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        options.setJobName("PubSubTopicToBigQuery - " + System.currentTimeMillis());



        PCollection<CommonLog> commonLogs = pipeline
                .apply("ReadMessage", PubsubIO.readStrings()
                        .withTimestampAttribute("timestamp")
                        .fromTopic(options.getInputTopic()))

                .apply("ParseJson", ParDo.of(new JsonToCommonLog()));

        // Window and write to BQ
        commonLogs
                .apply("WindowByMinute", Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowDuration()))))

                // update to Group.globally() after resolved: https://issues.apache.org/jira/browse/BEAM-10297
                // Only if supports Row output
                .apply("CountPerMinute", Combine.globally(Count.<CommonLog>combineFn()).withoutDefaults())
                .apply("ConvertToRow", ParDo.of(new DoFn<Long, Row>() {
                    @ProcessElement
                    public void processElement(@Element Long views, OutputReceiver<Row> r, IntervalWindow window) {
                        Instant i = Instant.ofEpochMilli(window.start().getMillis());
                        Row row = Row.withSchema(TableSchema)
                                .addValues(views, i)
                                .build();
                        r.output(row);
                    }
                })).setRowSchema(TableSchema)
                // Streaming insert of aggregate data
                .apply("WriteAggregateToBQ",
                        BigQueryIO.<Row>write().to(options.getAggregateTableName()).useBeamSchema()
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));



        LOG.info("Building pipeline...");

        return pipeline.run();
    }

}
