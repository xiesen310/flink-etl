package fx.etl.mock.validator;

import fx.etl.connector.ConnectorType;
import fx.etl.connector.console.ConsoleConfig;
import fx.etl.kafka.KafkaConfig;
import fx.etl.metadata.MetaData;
import fx.etl.metadata.MetadataContext;
import fx.etl.mock.MockData;
import fx.etl.mock.MockDto;
import fx.etl.mock.MockKafka;
import fx.etl.mock.MockSchema;
import fx.etl.validate.Validator;

import java.util.Map;
import java.util.Set;

/**
 * @author zhangdekun on 2019/2/28.
 */
public class SimpleValidator extends Validator {
    private String source =  "phonecontacts";
    private String output = "print";
    @Override
    public Map<String, String> udf() {
        return super.udf();
    }

    @Override
    public Map<String, MetadataContext> metadataContext() {
        //source
        MetadataContext phoneContactContext = new MetadataContext(source,null);
        phoneContactContext.setConnectorType(ConnectorType.KAFKA);
        MetaData phoneContactData = phoneContactContext.getMetaData();
        phoneContactData.setJsonSchema(MockSchema.sourceComplexJsonSchema());

        phoneContactData.getProperties().put(KafkaConfig.KAFKA_VERSION_KEY, MockKafka.KAFKA_VERSION);
        phoneContactData.getProperties().put(KafkaConfig.KAFKA_TOPIC_KEY,MockKafka.Topic.PHONE_CONTACT.toString());
        phoneContactData.getProperties().put(KafkaConfig.BOOTSTRAP_SERVER_KEY,MockKafka.KAFKA_BOOTSTRAP_SERVER);

        super.metadataContext().put(source,phoneContactContext);

        //output
        MetadataContext printContext = new MetadataContext(output,null);
        printContext.setConnectorType(ConnectorType.CONSOLE);
        MetaData printMetaData = printContext.getMetaData();
        printMetaData.setJsonSchema(MockSchema.targetSimpleJsonSchema());

        printMetaData.getProperties().put(ConsoleConfig.CONNECT_VERSION_KEY, ConsoleConfig.CONNECT_VERSION);

        super.metadataContext().put(output,printContext);
        return super.metadataContext();
    }

    @Override
    public Set<String> getSources() {
        super.getSources().add(source);
        return super.getSources();
    }

    @Override
    public Set<String> getOutputs() {
        super.getOutputs().add(output);
        return super.getOutputs();
    }
}
