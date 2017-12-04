package luna.applier;

import luna.common.AbstractLifeCycle;
import luna.common.context.MysqlContext;
import luna.common.db.sql.SqlTemplates;
import luna.common.model.meta.ColumnValue;
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
    private Map<SchemaTable, TableSqlUnit>      insertSqlCache;
    private Map<SchemaTable, TableSqlUnit>      updateSqlCache;
    private Map<SchemaTable, TableSqlUnit>      deleteSqlCache;
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
        logger.info("MysqlApplier is started!");
    }

    public void stop(){
        super.stop();
        insertSqlCache.clear();
        updateSqlCache.clear();
        deleteSqlCache.clear();
        logger.info("MysqlApplier is stopped!");
    }

    public void apply(Record record){
        DataSource dataSource = mysqlContext.getTargetDs().get(new SchemaTable(record.getSchema(),record.getTable()));
        apply(record,dataSource);
    }

    public void apply(Record record, DataSource dataSource){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        applyRecord(record, jdbcTemplate);
    }

    private void applyRecord(Record record,JdbcTemplate jdbcTemplate){
        TableSqlUnit sqlUnit = getSqlUnit(record);
        String applierSql = sqlUnit.applierSql;
        Map<String,Integer> indexs = sqlUnit.applierIndex;
        jdbcTemplate.execute(applierSql, new PreparedStatementCallback() {

            public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                List<ColumnValue> cvs = record.getColumns();
                for (ColumnValue cv : cvs) {
                    int index = indexs.get(cv.getColumn().getName());
                    if(index!=-1){
                        //, cv.getColumn().getType()
                        ps.setObject(indexs.get(cv.getColumn().getName()), cv.getValue());
                    }
                }
                try {
                    ps.execute();
                } catch (SQLException e) {
                    throw new SQLException("failed Record Data : " + record.toString(), e);
                }
                logger.info("Record: Has applied to mysql!");
                return null;
            }

        });
    }

    private TableSqlUnit getSqlUnit(Record record) {
        switch (record.getOperateType()) {
            case I:
                return getInsertSqlUnit(record);
            case U:
                return getUpdateSqlUnit(record);
            case D:
                return getDeleteSqlUnit(record);
            default:
                throw new LunaException("Unknown opType " + record.getOperateType());
        }
    }

    private TableSqlUnit getInsertSqlUnit(Record record) {
        SchemaTable schemaTable = new SchemaTable(record.getSchema(), record.getTable());
        TableSqlUnit sqlUnit = insertSqlCache.get(schemaTable);
        //不会多线程同时请求同一个SchemaTable，如果要考虑这种情况，可以synchronized(this){}，但是并发度会降低
        if (sqlUnit == null) {
            sqlUnit = new TableSqlUnit();
            String applierSql;
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

        return sqlUnit;
    }

    private TableSqlUnit getUpdateSqlUnit(Record record) {
        SchemaTable schemaTable = new SchemaTable(record.getSchema(), record.getTable());
        TableSqlUnit sqlUnit = updateSqlCache.get(schemaTable);
        //不会多线程同时请求同一个SchemaTable，如果要考虑这种情况，可以synchronized(this){}，但是并发度会降低
        if (sqlUnit == null) {
            sqlUnit = new TableSqlUnit();
            String applierSql;
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

        return sqlUnit;
    }


    private TableSqlUnit getDeleteSqlUnit(Record record) {
        SchemaTable schemaTable = new SchemaTable(record.getSchema(), record.getTable());
        TableSqlUnit sqlUnit = deleteSqlCache.get(schemaTable);
        //不会多线程同时请求同一个SchemaTable，如果要考虑这种情况，可以synchronized(this){}，但是并发度会降低
        if (sqlUnit == null) {
            sqlUnit = new TableSqlUnit();
            String applierSql;
            String[] primaryKey = {record.getPrimaryKey()};
            applierSql = SqlTemplates.getMYSQL().getDeleteSql(record.getSchema(),
                    record.getTable(), primaryKey);
            sqlUnit.applierSql = applierSql;
            sqlUnit.applierIndex = new HashMap<>();
            sqlUnit.applierIndex.put(record.getPrimaryKey(),1);
            for(String col:record.getcolumnNames()){
                if(!col.equalsIgnoreCase(record.getPrimaryKey())){
                    sqlUnit.applierIndex.put(col,-1);
                }
            }
            deleteSqlCache.put(schemaTable, sqlUnit);
        }

        return sqlUnit;
    }
}
