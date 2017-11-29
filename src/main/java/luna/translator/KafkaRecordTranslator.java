package luna.translator;

import luna.common.AbstractLifeCycle;
import luna.common.context.MysqlContext;
import luna.common.db.meta.ColumnMeta;
import luna.common.db.meta.ColumnValue;
import luna.common.db.meta.TableMeta;
import luna.common.model.OperateType;
import luna.common.model.Record;
import luna.common.model.SchemaTable;
import luna.applier.MysqlApplier;

import java.util.Map;

public class KafkaRecordTranslator extends AbstractLifeCycle implements Translator {
    private MysqlContext    mysqlContext;
    private MysqlApplier    mysqlApplier;

    public KafkaRecordTranslator(MysqlContext mysqlContext, MysqlApplier mysqlApplier){
        this.mysqlContext = mysqlContext;
        this.mysqlApplier = mysqlApplier;
    }

    public void start(){
        super.start();
    }

    public void stop(){
        super.stop();
    }


    public void translate(Map<String, Object> payload) throws Exception{
        String type = (String) payload.get("type");
        String schema = (String) payload.get("database");
        String tableName = (String) payload.get("table");
        Map<String,Object> recordPayload = (Map<String, Object>) payload.get("data");
        SchemaTable schemaTable = new SchemaTable(schema,tableName);
        TableMeta tableMeta = mysqlContext.getTableMetas().get(schemaTable);
        //SplitRule splitRule = mysqlContext.getTableSplitRule().get(schemaTable);
        int splitColumnValue = (int)(long)recordPayload.get(tableMeta.getExtKey());
        int targetNum = splitColumnValue%tableMeta.getExtNum();
        String targetSchema = schema+targetNum;
        String targetTable = tableName;
        Record record = new Record(targetSchema,targetTable,tableMeta.getPrimaryKey(),getOpType(type));
        recordPayload.forEach((columnName,columnValue)->{
            ColumnMeta columnMeta=tableMeta.getColumnMeta(columnName);
            ColumnValue column = new ColumnValue(columnMeta,columnValue);
            record.addColumn(column);
        });
        mysqlApplier.apply(record);
    }

    protected OperateType getOpType(String type){
        switch (type){
            case "insert":
                return OperateType.I;
            case "uodate":
                return OperateType.U;
            case "delete":
                return OperateType.D;
            default:
                return OperateType.UNKNOWN;
        }

    }
}
