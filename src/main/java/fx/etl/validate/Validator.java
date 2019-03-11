package fx.etl.validate;

import fx.etl.metadata.MetadataContext;
import fx.etl.mock.MockData;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import scala.tools.nsc.doc.model.Val;

import java.util.*;

/**
 * @author zhangdekun on 2019/2/20.
 */
public class Validator {
    private  String originSql ;
    private  String validatedSql;
    private Set<String> sources;
    private Set<String> outputs;
    private Map<String,String> udf = new HashMap<>();

    private Map<String,MetadataContext> metadataContextMap;
    public Validator(){

    }

    public Validator validate(String sql){
        this.originSql = sql;
        //todo
        SqlParser sqlParser = SqlParser.create(originSql);
        try {
            SqlNode sqlNode = sqlParser.parseStmt();

        } catch (SqlParseException e) {
            e.printStackTrace();
        }
        validatedSql = originSql;
        this.metadataContextMap = new HashMap<>();
        this.sources = new HashSet<>();
        this.outputs = new HashSet<>();
        return this;
    }

    public String validatedSql(){
        return validatedSql;
    }

    public Map<String,String> udf(){
        return udf;
    }

    public Map<String,MetadataContext> metadataContext(){

        return metadataContextMap;
    }

    public Set<String> getSources() {
        return sources;
    }

    public Set<String> getOutputs() {

        return outputs;
    }
}
