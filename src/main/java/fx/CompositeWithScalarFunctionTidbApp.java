package fx;

import fx.etl.mock.MockSql;
import fx.etl.mock.validator.CompositeValidator;
import fx.etl.mock.validator.TidbCompositeValidator;
import fx.etl.validate.Validator;

/**
 * @author zhangdekun on 2019/2/28.
 * kafka中数据有嵌套，嵌套数据抽取，插入到tidb
 */
public class CompositeWithScalarFunctionTidbApp {
    public static void main(String[] args) throws Exception{
        String sql = MockSql.mockCompositeSqlWithScalarFunction();
        Validator validator = new TidbCompositeValidator();
        App.run(sql,validator);
    }
}
