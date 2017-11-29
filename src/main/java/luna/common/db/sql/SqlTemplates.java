package luna.common.db.sql;

public class SqlTemplates {
    private static MysqlTemplate  MYSQL  = new MysqlTemplate();

    public static MysqlTemplate getMYSQL(){
        return MYSQL;
    }
}
