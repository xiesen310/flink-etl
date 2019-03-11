package fx.flink;

import org.apache.calcite.sql.SqlNodeList;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.sinks.TableSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author zhangdekun on 2019/2/14.
 */
public class Application {
    private Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {

    }

    private JobCompilerResult compile(Map<String, ExternalCatalog> inputs, String originSql,
                                      ExternalCatalog output, ResourceDTO resourceDTO,
                                      Configuration flinkConf) throws Exception {
        // 解析sql
        logger.info("to be compiled SqlApp : [{}]", originSql);
        SqlNodeList stmts = new CalciteSqlParser().parse(originSql);
        Validator validator = new Validator();

        validator.validateQuery(stmts);
        Map<String, String> udfMap = validator.getUserDefinedFunctions();
        String selectSql = validator.getStatement().toString();
        List additionalResources = validator.getAdditionalResources();
        logger.info("succeed to parse SqlApp,result is : [{}]", stmts);
        logger.info("udf {}", udfMap);
        logger.info("statement {}", selectSql);
        logger.info("additionalResources {}", additionalResources);
        // 准备编译,输出Flink的JobGraph
        logger.info("begin to create execution environment");
        StreamExecutionEnvironment localExecEnv = StreamExecutionEnvironment
                .createLocalEnvironment();
        //非常重要
        setFeature(localExecEnv,
                resourceDTO.getTaskManagerCount() * resourceDTO.getSlotPerTaskManager(), flinkConf);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(localExecEnv);
        logger.info("tableEnv : {} ", tableEnv);
        // 注册UDF,收归到平台了,也就是说,只支持平台开发人员预定义,暂时不支持业务自定义
        for (Map.Entry<String, String> e : udfMap.entrySet()) {
            final String name = e.getKey();
            String clazzName = e.getValue();
            logger.info("used udf specified by business : {}", name);
        }
        //registerSDF(athenaxCluster, tableEnv);
        logger.info("all udf registerd , bingo");
        // 开始注册所有的input相关的schema
        for (Map.Entry<String, ExternalCatalog> e : inputs.entrySet()) {
            logger.info("Registering input catalog {}", e.getKey());
            tableEnv.registerExternalCatalog(e.getKey(), e.getValue());
        }
        logger.info("all input catalog registerd , bingo");
        Table table = tableEnv.sqlQuery(selectSql);
        logger.info("succeed to execute tableEnv.sqlQuery(...)");
        logger.info("table {}", table);
        logger.info("bingo! input work done completely,let us handle output work now!!!");
// 开始注册output
        List<String> outputTables = output.listTables();
        for (String t : outputTables) {
            tableEnv.writeToSink(table,getOutputTable(output.getTable(t)),tableEnv.queryConfig());
        }
        logger.info("handle output ok");
// 生成JobGraph
        StreamGraph streamGraph = localExecEnv.getStreamGraph();
        JobGraph jobGraph = streamGraph.getJobGraph();
// this is required because the slots are allocated lazily
//如果为true就会报错,然后flink内部就是一直重启,所以设置为false
        jobGraph.setAllowQueuedScheduling(false);
        logger.info("create flink job ok {}", jobGraph);
       // JobGraphTool.analyze(jobGraph);
// 生成返回结果
        JobCompilerResult jobCompilerResult = new JobCompilerResult();
        jobCompilerResult.setJobGraph(jobGraph);
        List<String> paths = new ArrayList();
        Collection<String> values = udfMap.values();
        for (String value : values) {
            paths.add(value);
        }
        jobCompilerResult.setAdditionalJars(paths);
        return jobCompilerResult;
    }

    public void setFeature(StreamExecutionEnvironment env, int slots, Configuration configuration) {

    }

    public TableSink getOutputTable(ExternalCatalogTable table){
        return null;
    }
}
