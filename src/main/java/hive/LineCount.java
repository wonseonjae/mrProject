package hive;

import hive.conn.HiveConn;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class LineCount {
    public static void main(String[] args) throws Exception{
        Connection conn = null;

        try {
            conn = HiveConn.getDBConnection();

            PreparedStatement pstmt = null;

            String sql = "select count(line_data) as cnt from hivedb.comedies";

            pstmt = conn.prepareStatement(sql);

            ResultSet rs = pstmt.executeQuery(sql);

            if (rs.next()) {
                String cnt = rs.getString("cnt");
                System.out.println("cnt : " + cnt);
            }

            pstmt = null;
        }catch (Exception e) {
            System.out.println("error occurred " +e);
        }finally {
            HiveConn.dbClose(conn);
        }
    }
}
