package fx.etl.register;

import fx.etl.connector.ConnectorType;
import fx.etl.connector.console.Console;
import fx.etl.connector.console.ConsoleConfig;
import fx.etl.connector.tidb.Tidb;
import fx.etl.kafka.KafkaConfig;
import fx.etl.metadata.MetaData;
import fx.etl.metadata.MetadataContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @author zhangdekun on 2019/2/28.
 */
public class StreamRegister {
    public static void registerSource(StreamTableEnvironment tableEnv, MetadataContext metadataContext) {
        StreamTableDescriptor streamTableDescriptor = find(tableEnv,metadataContext,false);
        streamTableDescriptor.registerTableSource(metadataContext.getAliasName() == null ? metadataContext.getName() : metadataContext.getAliasName());
    }

    public static void registerSink(StreamTableEnvironment tableEnv, MetadataContext metadataContext) {
        StreamTableDescriptor streamTableDescriptor = find(tableEnv,metadataContext,true);
        streamTableDescriptor.registerTableSink(metadataContext.getAliasName() == null ? metadataContext.getName() : metadataContext.getAliasName());
    }
    public static StreamTableDescriptor find(StreamTableEnvironment tableEnv, MetadataContext metadataContext,boolean isSink){
        StreamTableDescriptor streamTableDescriptor ;
        ConnectorType connectorType = metadataContext.getConnectorType();
        MetaData metaData = metadataContext.usedMetaData();
        if(isSink){
            metaData = metadataContext.queryResultMetaData();
        }
        switch (connectorType) {
            case KAFKA:
                streamTableDescriptor = kafka(tableEnv, metaData);
                break;
            case CONSOLE:
                streamTableDescriptor = console(tableEnv,metaData);
                break;
            case TIDB:
                streamTableDescriptor = tidb(tableEnv,metaData);
                break;
            case MYSQL:
            default:
                throw new RuntimeException("not support");
        }
        return streamTableDescriptor;
    }
    public static StreamTableDescriptor kafka(StreamTableEnvironment tableEnv, MetaData metaData) {

        Properties properties = metaData.getProperties();
        String topic = properties.getProperty(KafkaConfig.KAFKA_TOPIC_KEY);
        String version = properties.getProperty(KafkaConfig.KAFKA_VERSION_KEY);

        StreamTableDescriptor streamTableDescriptor = tableEnv.connect(new Kafka()
                .version(version)
                .topic(topic)
                .startFromLatest()
                .properties(properties)
        )
                //todo format from metadata
                .withFormat(new Json().deriveSchema())
                .inAppendMode()
                //todo schema from metadata
                .withSchema(schema(metaData.getJsonSchema()));
        return streamTableDescriptor;
    }

    public static StreamTableDescriptor console(StreamTableEnvironment tableEnv, MetaData metaData) {

        Properties properties = metaData.getProperties();

        StreamTableDescriptor streamTableDescriptor = tableEnv.connect(new Console()
                .version(properties.getProperty(ConnectorDescriptorValidator.CONNECTOR_VERSION))
        )
                .withFormat(new Json().deriveSchema())
                .inAppendMode()
                .withSchema(schema(metaData.getJsonSchema()));
        return streamTableDescriptor;
    }
    public static StreamTableDescriptor tidb(StreamTableEnvironment tableEnv, MetaData metaData) {

        Properties properties = metaData.getProperties();

        StreamTableDescriptor streamTableDescriptor = tableEnv.connect(new Tidb()
                .version(properties.getProperty(ConnectorDescriptorValidator.CONNECTOR_VERSION))
                .properties(properties)
        )
                .withFormat(new Json().deriveSchema())
                .inAppendMode()
                .withSchema(schema(metaData.getJsonSchema()));
        return streamTableDescriptor;
    }
    public static Schema schema(String jsonSchema) {
        Schema schema = new Schema();
        TypeInformation<Row> row = JsonRowSchemaConverter.convert(jsonSchema);
        RowTypeInfo rowTypeInfo = (RowTypeInfo) row;
        String[] fieldNames = rowTypeInfo.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            schema.field(fieldNames[i], rowTypeInfo.getFieldTypes()[i]);
        }
        return schema;
    }
}
