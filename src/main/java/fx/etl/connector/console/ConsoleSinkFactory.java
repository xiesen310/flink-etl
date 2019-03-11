package fx.etl.connector.console;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.*;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.SchemaValidator.*;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;

/**
 * @author zhangdekun on 2019/2/20.
 */
public class ConsoleSinkFactory implements StreamTableSinkFactory<Row> {
    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        final TableSchema schema = descriptorProperties.getTableSchema(SCHEMA());
        return new ConsoleSink(schema);
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(UPDATE_MODE(), UPDATE_MODE_VALUE_APPEND());
        context.put(CONNECTOR_TYPE, "console");
        context.put(CONNECTOR_VERSION, "1.0");
        context.put(CONNECTOR_PROPERTY_VERSION, "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        // schema
        properties.add(SCHEMA() + ".#." + SCHEMA_TYPE());
        properties.add(SCHEMA() + ".#." + SCHEMA_NAME());
        properties.add(SCHEMA() + ".#." + SCHEMA_FROM());

        // format wildcard
        properties.add(FORMAT + ".*");
        return properties;
    }
}
