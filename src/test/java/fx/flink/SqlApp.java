package fx.flink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @author zhangdekun on 2019/2/14.
 */
public class SqlApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        /**
         * 1.inputs
         *   sql + 执行参数
         * 2.validator
         *   关联metadata + udf解析(使用aviator建立可以动态编辑的udf)
         * 3.builder lineage
         * 4.根据metadata建立sql中涉及到的source
         * 5.建立connect
         * 6.执行sql
         *
         */

        String jsonSchema = complexJsonSchema();

        JSONObject data = JSONObject.parseObject(jsonSchema).getJSONObject("properties").getJSONObject("data");

        TypeInformation<Row> rootRow = JsonRowSchemaConverter.convert(data.toJSONString());

        String sql = "select domain,data.user_id,data.innerData.d1 from phonecontacts";


        tableEnv.connect(new Kafka()
                .version("0.10")
                .topic("phonecontacts-topic")
                .startFromLatest()
                .properties(new Properties())
        )
                .withFormat(new Json().jsonSchema(jsonSchema))
                .inAppendMode()
                .withSchema(new Schema()

                        .field("data",rootRow)
                        .field("domain", Types.STRING))
                .registerTableSource("phonecontacts");

        Table table = tableEnv.sqlQuery(sql);

        TableSchema schema = table.getSchema();
        System.out.println(schema);

        TypeInformation<Row> resultRow = JsonRowSchemaConverter.convert(simpleJsonSchema());
        tableEnv.toAppendStream(table,resultRow).print();
        env.execute();

    }


    private static String simpleJsonSchema() {
        return "{" +
                "type:'object'," +
                "properties:{" +
                "domain:{type:'string'}," +
                "d1:{type:'string'}," +
                "user_id:{type:'string'}" +
                "}" +
                "}";
    }

    private static String complexJsonSchema() {
        return "{" +
                "type:'object'," +
                "properties:{" +
                "domain:{type:'string'}," +
                "data:{" +
                "type:'object'," +
                "properties:{" +
                "innerData:{" +
                "type:'object'," +
                "properties:{" +
                "d1:{type:'string'}" +
                "}"+
                "},"+
                "user_id:{type:'string'}," +
                "name:{type:'string'}," +
                "tel:{type:'string'}," +
                "mac:{type:'string'}" +
                "}}" +
                "}" +
                "}";
    }
}
