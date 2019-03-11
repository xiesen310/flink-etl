package fx.etl.connector.console;

import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;

import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;

/**
 * @author zhangdekun on 2019/2/20.
 */
public class Console extends ConnectorDescriptor {
    private String version;
    public Console() {
        super("console", 1, true);
    }

    public Console version(String version){
        Preconditions.checkNotNull(version);
        this.version = version;
        return this;
    }
    @Override
    protected Map<String, String> toConnectorProperties() {
        final DescriptorProperties properties = new DescriptorProperties();
        if (version != null) {
            properties.putString(CONNECTOR_VERSION, version);
        }
        return properties.asMap();
    }
}
