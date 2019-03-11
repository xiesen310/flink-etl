package fx.etl.connector.tidb;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

/**
 * @author zhangdekun on 2019/3/5.
 */
public class TidbSinkFunction extends RichSinkFunction<Row> implements CheckpointedFunction {
    private JDBCOutputFormat outputFormat;
    public TidbSinkFunction(JDBCOutputFormat outputFormat){
        this.outputFormat = outputFormat;
    }
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        outputFormat.flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        outputFormat.setRuntimeContext(runtimeContext);
        outputFormat.open(runtimeContext.getIndexOfThisSubtask(),runtimeContext.getNumberOfParallelSubtasks());
    }


    @Override
    public void invoke(Row value, Context context) throws Exception {
        outputFormat.writeRecord(value);
    }

    @Override
    public void close() throws Exception {
        outputFormat.close();
        super.close();
    }
}
