package luna.common.db.meta;

import luna.util.StringStyle;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.HashMap;
import java.util.Map;

public class TableMeta {
    private String                  schema;
    private String                  name;
    private String                  primaryKey;
    private Map<String,ColumnMeta>  columnMetas = new HashMap<>();
    private String                  extKey;
    private int                     extNum;

    public TableMeta(String schema, String name){
        this.schema = schema;
        this.name = name;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String,ColumnMeta> getColumns() {
        return columnMetas;
    }

    public void addColumn(String columnName,ColumnMeta columnMeta) {
        this.columnMetas.put(columnName,columnMeta);
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    /**
     * 返回schema.name
     */
    public String getFullName() {
        return schema + "." + name;
    }

    public String getExtKey() {
        return extKey;
    }

    public void setExtKey(String extKey) {
        this.extKey = extKey;
    }

    public int getExtNum() {
        return extNum;
    }

    public void setExtNum(int extNum) {
        this.extNum = extNum;
    }

    public ColumnMeta getColumnMeta(String columnName){
        return columnMetas.get(columnName);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, StringStyle.DEFAULT_STYLE);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        TableMeta other = (TableMeta) obj;
        if (name == null) {
            if (other.name != null) return false;
        } else if (!name.equals(other.name)) return false;
        if (schema == null) {
            if (other.schema != null) return false;
        } else if (!schema.equals(other.schema)) return false;
        return true;
    }
}
