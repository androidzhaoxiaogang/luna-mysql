package luna.common.model;

import com.google.common.collect.Lists;
import luna.common.model.meta.ColumnValue;
import luna.util.StringStyle;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.List;

public class Record implements Serializable{
    private static final long serialVersionUID = 4525923893891599547L;

    private String              schema;
    private String              table;
    private String              primaryKey;
    private OperateType         operateType;
    private List<ColumnValue>   columns = Lists.newArrayList();

    public Record(){}

    public Record(String schema, String table,String primaryKey,OperateType operateType){
        this.schema=schema;
        this.table=table;
        this.primaryKey=primaryKey;
        this.operateType=operateType;
    }

    public Record(String schema, String table, String primaryKey,OperateType operateType,List<ColumnValue>columns){
        this.schema=schema;
        this.table=table;
        this.primaryKey=primaryKey;
        this.columns=columns;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public List<ColumnValue> getColumns() {
        return columns;
    }

    public String getTable() {

        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public OperateType getOperateType() {
        return operateType;
    }

    public void setOperateType(OperateType operateType) {
        this.operateType = operateType;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public void addColumn(ColumnValue column) {
        columns.add(column);
    }

    public String[] getcolumnNames(){
        String[] result = new String[getColumns().size()];
        int i = 0;
        for (ColumnValue col : getColumns()) {
            result[i++] = col.getColumn().getName();
        }
        return result;
    }

    public int hashCode(){
        final int prime = 31;
        int result =1;
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        Record other = (Record) obj;
        if (schema == null) {
            if (other.schema != null) return false;
        } else if (!schema.equals(other.schema)) return false;
        if (table == null) {
            if (other.table != null) return false;
        } else if (!table.equals(other.table)) return false;
        if (primaryKey == null) {
            if (other.primaryKey != null) return false;
        } else if (!primaryKey.equals(other.primaryKey)) return false;
        if (operateType == null) {
            if (other.operateType != null) return false;
        } else if (!operateType.equals(other.operateType)) return false;
        if (columns == null) {
            if (other.columns != null) return false;
        } else if (!columns.equals(other.columns)) return false;
        return true;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, StringStyle.DEFAULT_STYLE);
    }


}
