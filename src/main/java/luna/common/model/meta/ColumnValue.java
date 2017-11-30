package luna.common.model.meta;

import java.io.Serializable;

public class ColumnValue implements Serializable{
    private static final long serialVersionUID = 6348862195538684854L;

    private ColumnMeta  column;
    private Object      value;

    public ColumnValue(){
    }

    public ColumnValue(ColumnMeta column, Object value){
        this.value = value;
        this.column = column;
    }

    public ColumnMeta getColumn() {
        return column;
    }

    public void setColumn(ColumnMeta column) {
        this.column = column;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public ColumnValue clone() {
        ColumnValue column = new ColumnValue();
        column.setValue(this.value);
        column.setColumn(this.column.clone());
        return column;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((column == null) ? 0 : column.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ColumnValue other = (ColumnValue) obj;
        if (column == null) {
            if (other.column != null) return false;
        } else if (!column.equals(other.column)) return false;
        if (value == null) {
            if (other.value != null) return false;
        } else if (!value.equals(other.value)) return false;
        return true;
    }

    @Override
    public String toString() {
        return "ColumnValue [column=" + column + ", value=" + value + "]";
    }

}
