package fx;

import fx.etl.metadata.MetadataContext;
import fx.etl.mock.MockData;
import fx.etl.register.StreamRegister;
import fx.etl.udf.UdfParser;
import fx.etl.validate.Validator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Map;
import java.util.Set;

/**
 * Hello world!
 * 数据集成，实时计算
 * @author zhangdekun
 */
public class App {
    public static void main(String[] args) throws Exception {

    }
    public static void run(String sql,Validator validatorInstance) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        /**
         * 接收变量sql
         * 多个sql使用分好隔开，保证从上到下的顺序
         * 前提条件是保证不同数据源或者相同数据源下的表名唯一
         */

        /**
         * sql语法验证 use calcite
         * 连接metadata
         * sql表以及字段验证
         * sql自定义函数验证
         */
        Validator validator = validatorInstance.validate(sql);

        /**
         * sql 源
         */
        Set<String> sources = validator.getSources();

        /**
         * sql结果输出
         */
        Set<String> outputs = validator.getOutputs();

        /**
         * customer udf
         */
        Map<String, String> udf = validator.udf();

        udf.forEach((identifier, udfStr) -> {
                    UdfParser udfParser = new UdfParser(udfStr);
                    registerUDF(tableEnv, identifier, udfParser);
                }
        );

        /**
         * 创建source的connect并注册source
         */
        Map<String, MetadataContext> metadataContexts = validator.metadataContext();

        sources.forEach(source -> StreamRegister.registerSource(tableEnv, metadataContexts.get(source)));

        /**
         * 创建sink的connect并注册
         */
        outputs.forEach(output -> StreamRegister.registerSink(tableEnv, metadataContexts.get(output)));


        /**
         * 数据处理
         */
        //multi sql
        //tableEnv.sqlQuery(validator.validatedSql());
        tableEnv.sqlUpdate(validator.validatedSql());
        StreamGraph streamGraph = env.getStreamGraph();
        //System.out.println(streamGraph.getStreamingPlanAsJSON());
        env.execute();
    }
    public static void registerUDF(final StreamTableEnvironment tableEnv, String identifier, UdfParser udfParser) {

        switch (udfParser.getUdfType()) {
            case UdfParser.SCALAR:
                tableEnv.registerFunction(identifier, udfParser.scalarFunction());
                break;
            case UdfParser.AGGREGATION:
                tableEnv.registerFunction(identifier, udfParser.aggregateFunction());
                break;
            case UdfParser.TABLE:
                tableEnv.registerFunction(identifier, udfParser.tableFunction());
                break;
            default:
                throw new RuntimeException("udf type :" + udfParser.getUdfType() + " not support.");
        }
    }
}
