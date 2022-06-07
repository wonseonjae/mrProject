package hive;

import java.sql.Connection;
import java.sql.DriverManager;

public class connTest {

    public static void main(String[] args) throws Exception{
        Connection conn = null;

        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");

            conn = DriverManager.getConnection("jdbc:hive2://192.168.52.130:10000/hivedb", "hadoop", "1234");
            System.out.println("Hive Connect!");
        }catch (ClassNotFoundException e) {
            System.out.println("hive Connect Fail");
            System.out.println("org.apache.hive.jdbc.HiveDriver file not found");

        }finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
}
