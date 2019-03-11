package fx;

import fx.etl.mock.MockSql;
import fx.etl.mock.validator.CompositeValidator;
import fx.etl.validate.Validator;

/**
 * @author zhangdekun on 2019/2/28.
 * kafka中数据有嵌套，嵌套数据抽取，打印到控制台
 */
public class CompositeWithScalarFunctionApp {
    public static void main(String[] args) throws Exception{
        String sql = MockSql.mockCompositeSqlWithScalarFunction();
        Validator validator = new CompositeValidator();
        App.run(sql,validator);
    }
}
