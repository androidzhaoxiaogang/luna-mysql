package luna.common.db.sql;

public class MysqlTemplate{

    private static final String DOT = ".";

    public String getMergeSql(String schemaName, String tableName, String[] pkNames, String[] colNames,
                              boolean mergeUpdatePk) {
        StringBuilder sql = new StringBuilder();
        sql.append("insert into ").append(makeFullName(schemaName, tableName)).append("(");
        int size = colNames.length;
        for (int i = 0; i < size; i++) {
            sql.append(getColumnName(colNames[i])).append(splitCommea(size, i));
        }

        sql.append(") values (");
        for (int i = 0; i < size; i++) {
            sql.append("?").append(splitCommea(size, i));
        }
        sql.append(") on duplicate key update ");

        // mysql merge sql匹配了uniqe / primary key时都会执行update，所以需要更新pk信息
        if (mergeUpdatePk) {
            for (int i = 0; i < size; i++) {
                sql.append(getColumnName(colNames[i]))
                        .append("=values(")
                        .append(getColumnName(colNames[i]))
                        .append(")");
                sql.append(splitCommea(size, i));
            }
        } else {
            // merge sql不更新主键信息, 规避drds情况下的分区键变更
            for (int i = 0; i < colNames.length; i++) {
                if(!inArray(colNames[i],pkNames)) {
                    sql.append(getColumnName(colNames[i]))
                            .append("=values(")
                            .append(getColumnName(colNames[i]))
                            .append(")");
                    sql.append(splitCommea(colNames.length, i));
                }
            }
        }

        // intern优化，避免出现大量相同的字符串
        return sql.toString().intern();
    }

    public String getInsertSql(String schemaName, String tableName,String[] columnNames) {
        StringBuilder sql = new StringBuilder();
        //ignore
        sql.append("insert into ").append(makeFullName(schemaName, tableName)).append("(");

        int size = columnNames.length;
        for (int i = 0; i < size; i++) {
            sql.append(getColumnName(columnNames[i])).append(splitCommea(size, i));
        }

        sql.append(") values (");
        makeColumnQuestions(sql, columnNames);
        sql.append(")");
        return sql.toString().intern();// intern优化，避免出现大量相同的字符串
    }

    public String getUpdateSql(String schemaName, String tableName, String [] pkNames, String[] columnNames) {
        StringBuilder sql = new StringBuilder();
        sql.append("update ").append(makeFullName(schemaName, tableName)).append(" set ");
        makeColumnEqualsSkip(sql, columnNames, ",",pkNames);
        sql.append(" where (");
        makeColumnEquals(sql, pkNames, "and");
        sql.append(")");
        return sql.toString().intern();
    }

    public String getDeleteSql(String schemaName, String tableName, String[] pkNames) {
        StringBuilder sql = new StringBuilder();
        sql.append("delete from ").append(makeFullName(schemaName, tableName)).append(" where ");
        makeColumnEquals(sql, pkNames, "and");
        // intern优化，避免出现大量相同的字符串
        return sql.toString().intern();
    }

    protected String makeFullName(String schemaName, String tableName) {
        String full = schemaName + DOT + tableName;
        return full.intern();
    }

    protected void makeColumnEquals(StringBuilder sql, String[] columns, String separator) {
        int size = columns.length;
        for (int i = 0; i < size; i++) {
            sql.append(" ").append(getColumnName(columns[i])).append(" = ").append("? ");
            if (i != size - 1) {
                sql.append(separator);
            }
        }
    }

    protected void makeColumnEqualsSkip(StringBuilder sql, String[] columns, String separator,String [] skip){
        int size = columns.length;
        for (int i = 0; i < size; i++) {
            if(!inArray(columns[i],skip)) {
                sql.append(" ").append(getColumnName(columns[i])).append(" = ").append("? ");
                if (i != size - 1) {
                    sql.append(separator);
                }
            }
        }
    }

    protected boolean inArray(String string,String [] array){
        for (String str:array){
            if(string.equalsIgnoreCase(str)){
                return true;
            }
        }
        return false;
    }

    protected void makeColumnQuestions(StringBuilder sql, String[] columns) {
        int size = columns.length;
        for (int i = 0; i < size; i++) {
            sql.append("?").append(splitCommea(size, i));
        }
    }

    protected String splitCommea(int size, int i) {
        return (i + 1 < size) ? " , " : "";
    }

    protected String getColumnName(String columName) {
        return "`" + columName + "`";
    }
}
