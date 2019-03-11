package fx.etl.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author zhangdekun on 2019/2/20.
 */
public class KafkaConfig {
    public static final String BOOTSTRAP_SERVER_KEY = ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
    public static final String KAFKA_VERSION_KEY = "kafka-version";
    public static final String KAFKA_TOPIC_KEY = "kafka-topic";
    private Properties properties = new Properties();
    public static KafkaConfig build(){
        KafkaConfig kafkaConfig = new KafkaConfig();

        return kafkaConfig;
    }

    public KafkaConfig bootstrapServer(String bootstrapServer){
        properties.put(BOOTSTRAP_SERVER_KEY,bootstrapServer);
        return this;
    }
    public  Properties properties(){
        return properties;
    }


}
