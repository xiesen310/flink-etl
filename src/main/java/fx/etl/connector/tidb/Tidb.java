package fx.etl.connector.tidb;

import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;

/**
 * @author zhangdekun on 2019/3/4.
 */
public class Tidb extends ConnectorDescriptor {
    private Map<String, String> properties;
    private String version;

    public Tidb() {
        super("tidb", 1, true);
    }

    public Tidb version(String version) {
        Preconditions.checkNotNull(version);
        this.version = version;
        return this;
    }

    public Tidb properties(Properties properties) {
        Preconditions.checkNotNull(properties);
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        properties.forEach((k, v) -> this.properties.put((String) k, (String) v));
        return this;
    }

    public Tidb property(String key, String value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        this.properties.put(key, value);
        return this;
    }

    @Override
    protected Map<String, String> toConnectorProperties() {
        final DescriptorProperties descriptorProperties = new DescriptorProperties();
        if (version != null) {
            descriptorProperties.putString(CONNECTOR_VERSION, version);
        }
        if (this.properties != null) {
            this.properties.forEach((k, v) -> {
                if(!descriptorProperties.containsKey(k)){
                    descriptorProperties.putString(k, v);
                }
            });
        }
        return descriptorProperties.asMap();
    }
}
