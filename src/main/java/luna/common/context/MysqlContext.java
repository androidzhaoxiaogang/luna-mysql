package luna.common.context;

import com.google.common.collect.Lists;
import luna.common.db.DataSourceConfig;
import luna.common.model.SchemaTable;
import luna.common.model.meta.TableMeta;

import javax.sql.DataSource;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MysqlContext implements Serializable{
    private static final long serialVersionUID = 4660863593558723238L;

    private DataSourceConfig                    sourceDsConfig;
    private Map<SchemaTable,DataSourceConfig>   targetDsConfigs = new HashMap<>();
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

    public DataSourceConfig getSourceDsConfig() {
        return sourceDsConfig;
    }

    public void setSourceDsConfig(DataSourceConfig sourceDsConfig) {
        this.sourceDsConfig = sourceDsConfig;
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

    public Map<SchemaTable, DataSourceConfig> getTargetDsConfigs() {
        return targetDsConfigs;
    }

    public void setTargetDsConfigs(Map<SchemaTable, DataSourceConfig> targetDsConfigs) {
        this.targetDsConfigs = targetDsConfigs;
    }

    public void putTargetDsConfig(SchemaTable schemaTable, DataSourceConfig dsConfig) {
        this.targetDsConfigs.put(schemaTable,dsConfig);
    }

}
