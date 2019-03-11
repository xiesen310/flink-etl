package fx.etl.connector.tidb;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.*;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_DEC_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

/**
 * @author zhangdekun on 2019/3/5.
 */
public class JDBCTypeUtil {
    private static final Map<TypeInformation<?>, Integer> TYPE_MAPPING;
    private static final Map<Integer, String> SQL_TYPE_NAMES;

    static {
        HashMap<TypeInformation<?>, Integer> m = new HashMap<>();
        m.put(STRING_TYPE_INFO, Types.VARCHAR);
        m.put(BOOLEAN_TYPE_INFO, Types.BOOLEAN);
        m.put(BYTE_TYPE_INFO, Types.TINYINT);
        m.put(SHORT_TYPE_INFO, Types.SMALLINT);
        m.put(INT_TYPE_INFO, Types.INTEGER);
        m.put(LONG_TYPE_INFO, Types.BIGINT);
        m.put(FLOAT_TYPE_INFO, Types.FLOAT);
        m.put(DOUBLE_TYPE_INFO, Types.DOUBLE);
        m.put(SqlTimeTypeInfo.DATE, Types.DATE);
        m.put(SqlTimeTypeInfo.TIME, Types.TIME);
        m.put(SqlTimeTypeInfo.TIMESTAMP, Types.TIMESTAMP);
        m.put(BIG_DEC_TYPE_INFO, Types.DECIMAL);
        m.put(BYTE_PRIMITIVE_ARRAY_TYPE_INFO, Types.BINARY);
        TYPE_MAPPING = Collections.unmodifiableMap(m);

        HashMap<Integer, String> names = new HashMap<>();
        names.put(Types.VARCHAR, "VARCHAR");
        names.put(Types.BOOLEAN, "BOOLEAN");
        names.put(Types.TINYINT, "TINYINT");
        names.put(Types.SMALLINT, "SMALLINT");
        names.put(Types.INTEGER, "INTEGER");
        names.put(Types.BIGINT, "BIGINT");
        names.put(Types.FLOAT, "FLOAT");
        names.put(Types.DOUBLE, "DOUBLE");
        names.put(Types.CHAR, "CHAR");
        names.put(Types.DATE, "DATE");
        names.put(Types.TIME, "TIME");
        names.put(Types.TIMESTAMP, "TIMESTAMP");
        names.put(Types.DECIMAL, "DECIMAL");
        names.put(Types.BINARY, "BINARY");
        SQL_TYPE_NAMES = Collections.unmodifiableMap(names);
    }

    private JDBCTypeUtil() {
    }

    static int typeInformationToSqlType(TypeInformation<?> type) {

        if (TYPE_MAPPING.containsKey(type)) {
            return TYPE_MAPPING.get(type);
        } else if (type instanceof ObjectArrayTypeInfo || type instanceof PrimitiveArrayTypeInfo) {
            return Types.ARRAY;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    static String getTypeName(int type) {
        return SQL_TYPE_NAMES.get(type);
    }

    static String getTypeName(TypeInformation<?> type) {
        return SQL_TYPE_NAMES.get(typeInformationToSqlType(type));
    }

}
