package fx.etl.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * @author zhangdekun on 2019/2/20.
 */
public class KafkaProducerMock{
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private  KafkaProducer<byte[], byte[]> producer = null;
    public KafkaProducerMock(Map<String,Object> config){
        producer = new KafkaProducer<byte[], byte[]>(config);
    }

    public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
        return producer.send(record);
    }

    public Future<RecordMetadata> send(String jsonString,String topic) {
        ProducerRecord<byte[],byte[]> data = new ProducerRecord<byte[], byte[]>(topic,jsonString.getBytes(UTF8));
        return send(data);
    }
}
