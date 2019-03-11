package fx.etl.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author zhangdekun on 2019/2/28.
 */
public class PrefixFunction extends ScalarFunction {
    public String eval(String str){
        return "prefix_"+str;
    }
}
