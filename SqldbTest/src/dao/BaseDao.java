package dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class BaseDao {
//	private static String driver="com.mysql.jdbc.Driver";
//    private static String url="jdbc:mysql://10.10.10.221:3306/relationship";
//	private static String url="jdbc:mysql://127.0.0.1:3306/relationship";
	private static String driver="com.mysql.cj.jdbc.Driver";
	private static String url="jdbc:mysql://localhost:3306/pictures?serverTimezone=GMT%2B8&amp";
    private static String user="root";
//    private static String password="123456";
    private static String password="Root123456#";
//	private static String driver="com.microsoft.sqlserver.jdbc.SQLServerDriver";
//	private final static String url = "jdbc:sqlserver://localhost:1433;DatabaseName=People";
//    private static final String user="sa";
//    private static final String password="123456";
        static {
            try {
                Class.forName(driver);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);    
    }
    
    public static void closeAll(Connection conn,Statement stmt,ResultSet rs) throws SQLException {
        if(rs!=null) {
            rs.close();
        }
        if(stmt!=null) {
            stmt.close();
        }
        if(conn!=null) {
            conn.close();
        }
    }
    
    
}
