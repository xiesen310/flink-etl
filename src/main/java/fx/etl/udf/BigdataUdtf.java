package fx.etl.udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author zhangdekun on 2019/2/26.
 */
public class BigdataUdtf extends TableFunction<Row> {
    @Override
    public void collect(Row row) {

        super.collect(row);
    }

    public void eval(Row datas) {
        if (datas != null) {
            System.out.println(datas);
        }
    }
    public void eval(String str) {
        System.out.println("udtf eval:"+str);
        collect(Row.of(str,str.length()));
    }
    public void eval(Row[] datas, Object... others) {
        if (others == null || others.length == 0) {
            eval(datas);
        } else {
            if (datas != null) {
                for (Row dataRow : datas) {
                    int arity = dataRow.getArity();
                    Row row = new Row(arity + others.length);
                    for (int i = 0; i < arity; i++) {
                        row.setField(i, dataRow.getField(i));
                    }
                    for (int i = 0; i < others.length; i++) {
                        row.setField(arity + i, others[i]);
                    }
                    collect(row);
                }
            }
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING(), Types.INT());
    }

}
