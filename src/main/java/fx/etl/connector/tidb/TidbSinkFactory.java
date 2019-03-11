package fx.etl.connector.tidb;

import org.apache.flink.api.common.typeinfo.TypeInformation;
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
 * @author zhangdekun on 2019/3/5.
 */
public class TidbSinkFactory implements StreamTableSinkFactory<Row> {
    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        final String username = descriptorProperties.getString(JDBCOptions.USER_NAME);
        final String password = descriptorProperties.getString(JDBCOptions.PASSWORD);
        final String url = descriptorProperties.getString(JDBCOptions.URL);
        final String driver = descriptorProperties.getString(JDBCOptions.DRIVER);
        final String batchInterval = descriptorProperties.getString(JDBCOptions.BATCH_INTERVAL);
        final String useSSL = descriptorProperties.getString(JDBCOptions.USE_SSL);

        final TableSchema schema = descriptorProperties.getTableSchema(SCHEMA());
        final String tableName = descriptorProperties.getString(JDBCOptions.TABLE_NAME);

        JDBCOutputFormat outputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
                .setBatchInterval(batchInterval == null ? null : Integer.parseInt(batchInterval))
                .setDBUrl(url)
                .setDrivername(driver)
                .setPassword(password)
                .setUseSSL(useSSL==null?"false":useSSL)
                .setQuery(sqlQuery(tableName, schema))
                .setSqlTypes(sqlTypes(schema))
                .setUsername(username)
                .finish();

        return new TidbSink(schema, outputFormat);
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(UPDATE_MODE(), UPDATE_MODE_VALUE_APPEND());
        context.put(CONNECTOR_TYPE, "tidb");
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

        //jdbc
        properties.add("jdbc.*");

        return properties;
    }

    private String sqlQuery(String tableName, TableSchema schema) {
        StringBuilder fields = new StringBuilder();
        StringBuilder question = new StringBuilder();
        String[] fieldNames = schema.getFieldNames();

        int count = 0;
        for (String fieldName : fieldNames) {
            if (count != 0) {
                fields.append(",");
                question.append(",");
            }
            fields.append(fieldName);
            question.append("?");
            count++;
        }
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, fields, question);
    }

    /**
     * return java.sql.types
     *
     * @param schema
     * @return
     */
    private int[] sqlTypes(TableSchema schema) {
        TypeInformation<?>[] fieldTypes = schema.getFieldTypes();
        int[] types = new int[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            types[i] = JDBCTypeUtil.typeInformationToSqlType(fieldTypes[i]);
        }
        return types;
    }
}
