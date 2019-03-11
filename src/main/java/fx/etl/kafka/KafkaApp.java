package fx.etl.kafka;

import fx.etl.mock.MockDto;
import fx.etl.mock.MockKafka;

import java.util.concurrent.ExecutionException;

/**
 * @author zhangdekun on 2019/2/20.
 */
public class KafkaApp {
    public static void main(String[] args) {
        KafkaProducerMock kafkaProducerMock = new KafkaProducerMock(MockKafka.producerConfigMap());
        try {
            kafkaProducerMock.send(MockDto.phoneConcactsJonsData(), MockKafka.Topic.PHONE_CONTACT.toString()).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
