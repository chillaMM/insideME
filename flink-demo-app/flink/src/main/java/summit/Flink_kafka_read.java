package summit;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class Flink_kafka_read {
    
    public static void main(String[] args) {
        
        String inputTopic = "summit-test-topic";
        String bootstrapServers = "";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<>



    }



}
