package luna.translator;

import luna.common.AbstractLifeCycle;
import luna.common.context.MysqlContext;
import luna.common.model.meta.ColumnMeta;
import luna.common.model.meta.ColumnValue;
import luna.common.model.meta.TableMeta;
import luna.common.model.OperateType;
import luna.common.model.Record;
import luna.common.model.SchemaTable;
import luna.applier.MysqlApplier;
import luna.exception.LunaException;

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
        logger.info("KafkaRecordTranslator is started!");
    }

    public void stop(){
        //super.stop();
        logger.info("KafkaRecordTranslator is stopped!");
    }


    public void translate(Map<String, Object> payload){
        String type = (String) payload.get("type");
        String schema = (String) payload.get("database");
        String tableName = (String) payload.get("table");
        Map<String,Object> recordPayload = (Map<String, Object>) payload.get("data");
        SchemaTable schemaTable = new SchemaTable(schema,tableName);
        TableMeta tableMeta = mysqlContext.getTableMetas().get(schemaTable);
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

    private OperateType getOpType(String type){
        switch (type){
            case "insert":
                return OperateType.I;
            case "update":
                return OperateType.U;
            case "delete":
                return OperateType.D;
            default:
                throw new LunaException("Unknown operation type!");
        }

    }
}
