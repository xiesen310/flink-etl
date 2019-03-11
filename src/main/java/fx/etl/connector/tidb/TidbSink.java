package fx.etl.connector.tidb;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.types.Row;

import java.util.Arrays;


/**
 * @author zhangdekun on 2019/3/5.
 */
public class TidbSink implements AppendStreamTableSink<Row> {
    private TableSchema schema;
    private JDBCOutputFormat outputFormat;

    public TidbSink(TableSchema schema, JDBCOutputFormat outputFormat) {
        this.schema = schema;
        this.outputFormat = outputFormat;
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return schema.toRowType();
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        dataStream.addSink(new TidbSinkFunction(outputFormat)).name("tidb");
    }

    @Override
    public String[] getFieldNames() {
        return schema.getFieldNames();
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return schema.getFieldTypes();
    }

    @Override
    public TidbSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        if (!Arrays.equals(getFieldNames(), fieldNames) || !Arrays.equals(getFieldTypes(), fieldTypes)) {
            throw new ValidationException("Reconfiguration with different fields is not allowed. " +
                    "Expected: " + Arrays.toString(getFieldNames()) + " / " + Arrays.toString(getFieldTypes()) + ". " +
                    "But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
        }
        return this;
    }

}
