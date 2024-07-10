package summit;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
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

public class KafkaToS3 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "b-1.democluster.v9nt77.c9.kafka.eu-west-1.amazonaws.com:9098,b-2.democluster.v9nt77.c9.kafka.eu-west-1.amazonaws.com:9098");
        kafkaProperties.setProperty("group.id", "flink-group");
        kafkaProperties.setProperty("security.protocol", "SASL_SSL");
        kafkaProperties.setProperty("sasl.mechanism", "AWS_MSK_IAM");
        kafkaProperties.setProperty("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        kafkaProperties.setProperty("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        kafkaProperties.setProperty("auto.offset.reset", "earliest");
        



        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "summit-test-topic",
                new SimpleStringSchema(),
                kafkaProperties
        );
        kafkaConsumer.setStartFromLatest();

        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("s3a://dtpl-gds-summit3-processed-messages/test_folder/"), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner("yyyy-MM-dd--HH"))
                .withRollingPolicy(
                DefaultRollingPolicy.create().build())
                .build();

        kafkaStream.addSink(sink);

        env.execute("Flink Kafka to S3");
    }
}



