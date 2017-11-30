package luna.common.model;

import luna.util.StringStyle;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;

public class SchemaTable implements Serializable{
    private static final long serialVersionUID = 2102370099863511331L;

    private String schema;
    private String table;

    public SchemaTable(String schema,String table){
        this.schema=schema;
        this.table=table;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public int hashCode(){
        final int prime = 31;
        int result =1;
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
        result = prime * result + ((table == null) ? 0 : table.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        SchemaTable other = (SchemaTable) obj;
        if (schema == null) {
            if (other.schema != null) return false;
        } else if (!schema.equals(other.schema)) return false;
        if (table == null) {
            if (other.table != null) return false;
        } else if (!table.equals(other.table)) return false;
        return true;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, StringStyle.DEFAULT_STYLE);
    }
}
