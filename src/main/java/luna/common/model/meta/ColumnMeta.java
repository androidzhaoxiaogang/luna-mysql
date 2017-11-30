package luna.common.model.meta;

import luna.util.StringStyle;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;

public class ColumnMeta implements Serializable{
    private static final long serialVersionUID = 2911727340486550457L;

    private String name;
    private int    type; //java.sql.Types

    public ColumnMeta(String columnName, int columnType){
        this.name = columnName;
        this.type = columnType;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(int type) {
        this.type = type;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + type;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ColumnMeta other = (ColumnMeta) obj;
        if (name == null) {
            if (other.name != null) return false;
        } else if (!name.equals(other.name)) return false;
        if (type != other.type) return false;
        return true;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, StringStyle.DEFAULT_STYLE);
    }

    @Override
    public ColumnMeta clone() {
        return new ColumnMeta(this.name, this.type);
    }

}
