package luna.common.context;

import com.google.common.collect.Lists;
import luna.common.model.SchemaTable;
import luna.common.db.meta.TableMeta;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MysqlContext {
    private DataSource                          sourceDs;
    private Map<SchemaTable,DataSource>         targetDs = new HashMap<>();
    private List<SchemaTable>                   sourceTables = Lists.newArrayList();
    private Map<SchemaTable,TableMeta>          tableMetas = new HashMap<>();

    public Map<SchemaTable, TableMeta> getTableMetas() {
        return tableMetas;
    }

    public void setTableMetas(Map<SchemaTable, TableMeta> tableMetas) {
        this.tableMetas = tableMetas;
    }

    public void putTableMeta(SchemaTable schemaTable, TableMeta tableMetas) {
        this.tableMetas.put(schemaTable,tableMetas);
    }

    public DataSource getSourceDs() {
        return sourceDs;
    }

    public void setSourceDs(DataSource sourceDs) {
        this.sourceDs = sourceDs;
    }

    public List<SchemaTable> getSourceTables() {
        return sourceTables;
    }

    public void setSourceTables(List<SchemaTable> sourceTables) {
        this.sourceTables = sourceTables;
    }

    public void addSourceTable(SchemaTable sourceTable) {
        this.sourceTables.add(sourceTable);
    }

    public Map<SchemaTable, DataSource> getTargetDs() {
        return targetDs;
    }

    public void setTargetDs(Map<SchemaTable, DataSource> targetDs) {
        this.targetDs = targetDs;
    }

    public void putTargetDs(SchemaTable schemaTable, DataSource targetDs) {
        this.targetDs.put(schemaTable,targetDs);
    }


}
