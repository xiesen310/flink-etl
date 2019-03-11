package fx.etl.mock;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.types.Row;

import java.util.HashSet;
import java.util.Set;

/**
 * @author zhangdekun on 2019/2/20.
 */
public class MockData {

    /**
     * 根据根据json的key查找子节点
     *
     * @param jsonSchema
     * @param nestKey    key的层级关系用点表示
     * @return
     */
    private static TypeInformation<Row> createTypeInformationFromJsonSchema(String jsonSchema, String nestKey) {
        JSONObject data = JSONObject.parseObject(jsonSchema);
        if (!StringUtils.isBlank(nestKey)) {
            String[] keys = nestKey.split("\\.");
            for (String key : keys) {
                data = data.getJSONObject(key);
            }
        }
        TypeInformation<Row> row = JsonRowSchemaConverter.convert(data.toJSONString());

        return row;
    }
    private static TypeInformation<Row> createTypeInformationFromJsonSchemaUntilArray(String jsonSchema, String nestKey) {
        JSONObject data = JSONObject.parseObject(jsonSchema);
        if (!StringUtils.isBlank(nestKey)) {
            String[] keys = nestKey.split("\\.");
            for (String key : keys) {
                Object o = data.get(key);
                if(o instanceof JSONArray){
                    return JsonRowSchemaConverter.convert(((JSONArray) o).toJSONString());
                }else {
                    data = (JSONObject)o;
                }

            }
        }
        throw new RuntimeException("can not find json array ");
    }


    /**
     * 返回表名，通过表名来选择输出
     * sink table name
     * @return
     */
    public static Set<String> outputs() {
        return new HashSet<String>(){
            {
                add("print");
            }
        };
    }

    /**
     * sources table name
     * @return
     */
    public static Set<String> sources() {
        return new HashSet<String>(){
            {
                add("phonecontacts");
            }
        };
    }




}
