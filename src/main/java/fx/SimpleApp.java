package fx;

import fx.etl.mock.MockSql;
import fx.etl.mock.validator.SimpleValidator;
import fx.etl.validate.Validator;

/**
 * @author zhangdekun on 2019/2/28.
 *         kafka中数据无嵌套，简单抽取部分字段，打印到控制台
 */
public class SimpleApp {
    public static void main(String[] args) throws Exception {
        String sql = MockSql.mockSimpleSql();
        Validator validator = new SimpleValidator();
        App.run(sql, validator);
    }
}
