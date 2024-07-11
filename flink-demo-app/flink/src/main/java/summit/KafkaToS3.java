package summit;
import java.util.HashSet;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class KafkaToS3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

      


        KafkaSource<String> kafkaConsumer =  KafkaSource.<String>builder()
        .setTopics("summit-topic")
        .setBootstrapServers( "b-1.democluster.v9nt77.c9.kafka.eu-west-1.amazonaws.com:9098,b-2.democluster.v9nt77.c9.kafka.eu-west-1.amazonaws.com:9098")
         .setGroupId( "flink-group")
         .setProperty("security.protocol", "SASL_SSL")
        .setProperty("sasl.mechanism", "AWS_MSK_IAM")
        .setProperty("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
        .setProperty("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema()).build();

        DataStream<String> kafkaStream = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(),  "Kafka Source" );

        final StreamingFileSink<String> sink = StreamingFileSink
                .<String>forRowFormat(new Path("s3a://dtpl-gds-summit3-processed-messages/test_folder/"), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner("yyyy-MM-dd--HH"))
                .withRollingPolicy(
                DefaultRollingPolicy.create().build())
                .build();

        kafkaStream.addSink(sink);

        env.execute("Flink Kafka to S3");
    }
}



