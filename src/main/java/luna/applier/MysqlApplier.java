package luna.applier;

import luna.common.AbstractLifeCycle;
import luna.common.context.MysqlContext;
import luna.common.db.sql.SqlTemplates;
import luna.common.db.meta.ColumnValue;
import luna.common.model.Record;
import luna.common.model.SchemaTable;
import luna.exception.LunaException;

import com.google.common.collect.MapMaker;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class MysqlApplier extends AbstractLifeCycle implements Applier{
    protected Map<SchemaTable, TableSqlUnit>   insertSqlCache;
    protected Map<SchemaTable, TableSqlUnit>   updateSqlCache;
    protected Map<SchemaTable, TableSqlUnit>   deleteSqlCache;
    private MysqlContext                        mysqlContext;
    private MapMaker                            concurrentMapMaker = new MapMaker();

    public MysqlApplier(MysqlContext mysqlContext){
        this.mysqlContext=mysqlContext;
    }

    public static class TableSqlUnit {
        public String               applierSql;
        public Map<String,Integer>  applierIndex;
    }


    public void start(){
        super.start();
        insertSqlCache = concurrentMapMaker.makeMap();
        updateSqlCache = concurrentMapMaker.makeMap();
        deleteSqlCache = concurrentMapMaker.makeMap();
    }

    public void stop(){
        super.stop();
        insertSqlCache.clear();
        updateSqlCache.clear();
        deleteSqlCache.clear();
    }

    public void apply(Record record){
        DataSource dataSource = mysqlContext.getTargetDs().get(new SchemaTable(record.getSchema(),record.getTable()));
        apply(record,dataSource);
    }

    public void apply(Record record, DataSource dataSource){
        doApply(record,dataSource);
    }

    protected void doApply(Record record,DataSource dataSource){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        applyRecord(record, jdbcTemplate);

    }

    protected void applyRecord(Record record,JdbcTemplate jdbcTemplate){
        TableSqlUnit sqlUnit = getSqlUnit(record);
        String applierSql = sqlUnit.applierSql;
        Map<String,Integer> indexs = sqlUnit.applierIndex;
        jdbcTemplate.execute(applierSql, new PreparedStatementCallback() {

            public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                List<ColumnValue> cvs = record.getColumns();
                for (ColumnValue cv : cvs) {
                    int index = indexs.get(cv.getColumn().getName());
                    if(index!=-1){
                        ps.setObject(indexs.get(cv.getColumn().getName()), cv.getValue(), cv.getColumn().getType());
                    }
                }
                try {
                    ps.execute();
                } catch (SQLException e) {
                    throw new SQLException("failed Record Data : " + record.toString(), e);
                }
                return null;
            }

        });
    }

    protected TableSqlUnit getSqlUnit(Record record) {
        switch (record.getOperateType()) {
            case I:
                return getInsertSqlUnit(record);
            case U:
                return getUpdateSqlUnit(record);
            case D:
                return getDeleteSqlUnit(record);
            default:
                break;
        }

        throw new LunaException("unknow opType " + record.getOperateType());
    }

    protected TableSqlUnit getInsertSqlUnit(Record record) {
        SchemaTable schemaTable = new SchemaTable(record.getSchema(), record.getTable());
        TableSqlUnit sqlUnit = insertSqlCache.get(schemaTable);
        if (sqlUnit == null) {
            synchronized (schemaTable) {
                sqlUnit = insertSqlCache.get(schemaTable);
                if (sqlUnit == null) { // double-check
                    sqlUnit = new TableSqlUnit();
                    String applierSql = null;
                    String[] columns = record.getcolumnNames();

                    applierSql = SqlTemplates.getMYSQL().getInsertSql(record.getSchema(),
                            record.getTable(),
                            columns);
                    Map<String,Integer>indexs = new HashMap<>();
                    int index=1;
                    for(String col:columns){
                        indexs.put(col,index);
                        index++;
                    }
                    sqlUnit.applierSql = applierSql;
                    sqlUnit.applierIndex = indexs;
                    insertSqlCache.put(schemaTable, sqlUnit);
                }
            }
        }

        return sqlUnit;
    }

    protected TableSqlUnit getUpdateSqlUnit(Record record) {
        SchemaTable schemaTable = new SchemaTable(record.getSchema(), record.getTable());
        TableSqlUnit sqlUnit = updateSqlCache.get(schemaTable);
        if (sqlUnit == null) {
            synchronized (schemaTable) {
                sqlUnit = updateSqlCache.get(schemaTable);
                if (sqlUnit == null) { // double-check
                    sqlUnit = new TableSqlUnit();
                    String applierSql = null;

                    String [] primaryKey = {record.getPrimaryKey()};
                    String[] columns = record.getcolumnNames();
                    applierSql = SqlTemplates.getMYSQL().getUpdateSql(record.getSchema(),
                            record.getTable(),
                            primaryKey,
                            columns);
                    Map<String,Integer>indexs = new HashMap<>();
                    int index = 1;
                    for(String col:columns){
                        if(!col.equalsIgnoreCase(record.getPrimaryKey())){
                            indexs.put(col,index);
                            index++;
                        }
                    }
                    indexs.put(record.getPrimaryKey(),index);
                    sqlUnit.applierSql = applierSql;
                    sqlUnit.applierIndex = indexs;
                    updateSqlCache.put(schemaTable, sqlUnit);
                }
            }
        }

        return sqlUnit;
    }


    protected TableSqlUnit getDeleteSqlUnit(Record record) {
        SchemaTable schemaTable = new SchemaTable(record.getSchema(), record.getTable());
        TableSqlUnit sqlUnit = deleteSqlCache.get(schemaTable);
        if (sqlUnit == null) {
            synchronized (schemaTable) {
                sqlUnit = deleteSqlCache.get(schemaTable);
                if (sqlUnit == null) { // double-check
                    sqlUnit = new TableSqlUnit();
                    String applierSql = null;
                    String[] primaryKey = {record.getPrimaryKey()};
                    applierSql = SqlTemplates.getMYSQL().getDeleteSql(record.getSchema(),
                            record.getTable(), primaryKey);
                    sqlUnit.applierSql = applierSql;
                    sqlUnit.applierIndex.put(record.getPrimaryKey(),1);
                    for(String col:record.getcolumnNames()){
                        if(!col.equalsIgnoreCase(record.getPrimaryKey())){
                            sqlUnit.applierIndex.put(col,-1);
                        }
                    }
                    deleteSqlCache.put(schemaTable, sqlUnit);
                }
            }
        }

        return sqlUnit;
    }
}
