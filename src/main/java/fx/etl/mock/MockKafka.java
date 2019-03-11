package fx.etl.mock;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhangdekun on 2019/2/28.
 */
public class MockKafka {

    public static final String KAFKA_VERSION = "0.10";
    public static final String KAFKA_BOOTSTRAP_SERVER = "192.168.33.117:9092";
    public enum Topic{
        PHONE_CONTACT("phonecontacts_topic");
        private String topic ;
        private Topic(String topic){
            this.topic = topic;
        }

        @Override
        public String toString() {
            return this.topic;
        }
    }


    public static Map<String,Object> producerConfigMap(){
        Map<String,Object> map = new HashMap<>();
        map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_BOOTSTRAP_SERVER);
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,ByteArraySerializer.class.getName());
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ByteArraySerializer.class.getName());
        map.put(ProducerConfig.ACKS_CONFIG,"0");
        return map;
    }
}


