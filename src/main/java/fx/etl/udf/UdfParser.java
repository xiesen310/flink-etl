package fx.etl.udf;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;

/**
 * @author zhangdekun on 2019/2/25.
 */
public class UdfParser {
    public static final String SCALAR = "scalar";
    public static final String TABLE = "table";
    public static final String AGGREGATION = "aggregation";
    private ScalarFunction scalarFunction = null;
    private TableFunction tableFunction = null;
    private AggregateFunction aggregateFunction = null;
    private String udfStr;

    public UdfParser(String udfStr){
        this.udfStr = udfStr;
        try {
            Class<?> aClass = Class.forName(udfStr);
            if(ScalarFunction.class.isAssignableFrom(aClass)){
                udfType = SCALAR;
                try {
                    scalarFunction = (ScalarFunction)aClass.newInstance();
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    private String udfType;
    public  ScalarFunction scalarFunction(){
        return scalarFunction;
    }
    public  TableFunction tableFunction(){
        return tableFunction;
    }
    public  AggregateFunction aggregateFunction(){
        return aggregateFunction;
    }

    public String getUdfType() {
        return udfType;
    }
}
