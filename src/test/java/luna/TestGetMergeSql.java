package luna;

import luna.common.db.sql.SqlTemplates;

public class TestGetMergeSql {
    public static void main(String[]args){
        String [] pk={"sss","aaaa"};
        String [] cols={"bbb","wwww","sss","aaaa"};
        System.out.print(SqlTemplates.getMYSQL().getMergeSql("schema","table",pk,cols,false));
    }
}
