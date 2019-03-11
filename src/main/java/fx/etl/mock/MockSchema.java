package fx.etl.mock;

/**
 * @author zhangdekun on 2019/2/28.
 */
public class MockSchema {
    public static String targetCompositeJsonSchema() {
        return "{" +
                "type:'object'," +
                "properties:{" +
                "domain:{type:'string'}," +
                "ip:{type:'string'}," +
                "d1:{type:'string'}," +
                "userId:{type:'string'}" +
                "}" +
                "}";
    }
    public static String targetSimpleJsonSchema() {
        return "{" +
                "type:'object'," +
                "properties:{" +
                "domain:{type:'string'}," +
                "ip:{type:'string'}" +
                "}" +
                "}";
    }
    public static String sourceComplexJsonSchema() {
        return "{" +
                "type:'object'," +
                "properties:{" +
                "domain:{type:'string'}," +
                "ip:{type:'string'}," +
                "data:{" +
                "type:'object'," +
                "properties:{" +
                "innerData:{" +
                "type:'object'," +
                "properties:{" +
                "d1:{type:'string'}" +
                "}" +
                "}," +
                "user_id:{type:'string'}," +
                "name:{type:'string'}," +
                "tel:{type:'string'}," +
                "mac:{type:'string'}" +
                "}" +
                "}" +
                "}" +
                "}";
    }
}
