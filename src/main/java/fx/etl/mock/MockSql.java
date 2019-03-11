package fx.etl.mock;

/**
 * @author zhangdekun on 2019/2/28.
 */
public class MockSql {
    public static String mockCompositeSql() {
        return "insert into print_composite select domain,ip,data.user_id,data.innerData.d1 from phonecontacts_composite";
    }
    public static String mockCompositeSqlWithScalarFunction() {
        return "insert into print_composite select domain,ip ,data.innerData.d1,prefix(data.user_id) as userId from phonecontacts_composite";
    }
    public static String project1(){
        return "domain,d1,userId,ip";
    }
    public static String mockSimpleSql() {
        return "insert into print select domain,ip from phonecontacts";
    }
}
