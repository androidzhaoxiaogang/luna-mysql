package luna.common.db;

import luna.common.model.meta.ColumnMeta;
import luna.common.model.meta.TableMeta;
import org.apache.commons.lang.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.*;


public class TableMetaGenerator {

    public static TableMeta buildColumns(DataSource dataSource, String schema, String tableName) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        TableMeta table = new TableMeta(schema,tableName);
        return (TableMeta)jdbcTemplate.execute(new ConnectionCallback(){

            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = conn.getMetaData();
                ResultSet rs;
                // 查询所有字段
                rs = metaData.getColumns(schema, schema, tableName, null);

                while (rs.next()) {
                    String columnName = rs.getString(4); // COLUMN_NAME
                    int columnType = rs.getInt(5);
                    String typeName = rs.getString(6);
                    columnType = convertSqlType(columnType, typeName);
                    ColumnMeta col = new ColumnMeta(columnName, columnType);
                    table.addColumn(columnName,col);

                }
                rs.close();

                // 查询主键信息
                rs = metaData.getPrimaryKeys(schema, schema, tableName);
                while (rs.next()) {
                    table.setPrimaryKey(rs.getString(4));
                }
                rs.close();
                return table;
            }

        });

    }

    private static int convertSqlType(int columnType, String typeName) {
        String[] typeSplit = typeName.split(" ");
        if (typeSplit.length > 1) {
            if (columnType == Types.INTEGER && StringUtils.equalsIgnoreCase(typeSplit[1], "UNSIGNED")) {
                columnType = Types.BIGINT;
            }
        }

        if (columnType == Types.OTHER) {
            if (StringUtils.equalsIgnoreCase(typeName, "NVARCHAR")
                    || StringUtils.equalsIgnoreCase(typeName, "NVARCHAR2")) {
                columnType = Types.VARCHAR;
            }

            if (StringUtils.equalsIgnoreCase(typeName, "NCLOB")) {
                columnType = Types.CLOB;
            }

            if (StringUtils.startsWithIgnoreCase(typeName, "TIMESTAMP")) {
                columnType = Types.TIMESTAMP;
            }
        }
        return columnType;
    }


}
