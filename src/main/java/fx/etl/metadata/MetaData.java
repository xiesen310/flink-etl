package fx.etl.metadata;

import java.util.Properties;

/**
 * @author zhangdekun on 2019/2/20.
 */
public class MetaData {
    private String jsonSchema;
    private String avroSchema;
    private Properties properties = new Properties();

    public String getJsonSchema() {
        return jsonSchema;
    }

    public void setJsonSchema(String jsonSchema) {
        this.jsonSchema = jsonSchema;
    }

    public String getAvroSchema() {
        return avroSchema;
    }

    public void setAvroSchema(String avroSchema) {
        this.avroSchema = avroSchema;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
