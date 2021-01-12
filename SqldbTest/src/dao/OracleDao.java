package dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OracleDao {

	private static String driver="oracle.jdbc.OracleDriver";
//	private static String url="jdbc:oracle:thin:@localhost:1521:orcl";
	private static String url="jdbc:oracle:thin:@172.16.13.63:1521:orcl";
	private static String user="myuser";
	private static String password="myuser";
	
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
