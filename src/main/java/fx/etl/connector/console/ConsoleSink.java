package fx.etl.connector.console;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * @author zhangdekun on 2019/2/20.
 */
public class ConsoleSink  implements AppendStreamTableSink<Row> {
    private TableSchema schema;

    public ConsoleSink(TableSchema schema){
        this.schema = schema;
    }
    @Override
    public TypeInformation<Row> getOutputType() {
        return schema.toRowType();
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        PrintSinkFunction<Row> printFunction = new PrintSinkFunction();
        dataStream.addSink(printFunction).name("Print to Std. Out");
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
    public ConsoleSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        if (!Arrays.equals(getFieldNames(), fieldNames) || !Arrays.equals(getFieldTypes(), fieldTypes)) {
            throw new ValidationException("Reconfiguration with different fields is not allowed. " +
                    "Expected: " + Arrays.toString(getFieldNames()) + " / " + Arrays.toString(getFieldTypes()) + ". " +
                    "But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
        }
        return this;
    }
}
