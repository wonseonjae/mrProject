package hive.conn;

import java.sql.Connection;
import java.sql.DriverManager;

public class HiveConn {

    public static Connection getDBConnection() throws Exception{
        Connection conn = null;

        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");

            conn = DriverManager.getConnection("jdbc:hive2://192.168.52.130:10000/hivedb", "hadoop", "1234");
            System.out.println("Hive Connect!");
        }catch (ClassNotFoundException e) {
            System.out.println("hive Connect Fail");
            System.out.println("org.apache.hive.jdbc.HiveDriver file not found");
            System.out.println("reson : " + e);
        }catch (Exception e) {
            System.out.println("Hive Connect Fail");
            System.out.println("Last Exception reach");
            System.out.println("reson : " + e);
        }

        return conn;
    }

    public static void dbClose(Connection conn) throws Exception{
        conn.close();
    }
}
