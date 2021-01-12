package semanticExpansion;

import java.util.ArrayList;
import java.util.List;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import dao.BaseDao;

public class SimNetwork {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<String> ids = new SimNetwork().getRelatedDis("DOID:0050168");
		for(String id : ids){
			System.out.println(id);
		}
	}
	
	public List<String> getRelatedDis(String doid){
		List<String> ids = new ArrayList<String>();
		PreparedStatement pstmt = null;
        ResultSet rs = null;
        Connection conn = null;
        String sql = "select * from disease_sim where id1 = ?  or id2 = ? order by y DESC ";
		try {
			conn = BaseDao.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, doid);
			pstmt.setString(2, doid);
			rs = pstmt.executeQuery();
			while(rs.next()){
				String id1 = rs.getString("id1");
				if(!id1.equals(doid)){
					ids.add(id1);
				}
				String id2 = rs.getString("id2");
				if(!id2.equals(doid)){
					ids.add(id2);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			try {
				BaseDao.closeAll(conn, pstmt, rs);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return ids;
	}
}
