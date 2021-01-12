package OracleTest;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import MysqlTest.StoreToMysql;
import dao.BaseDao;
import dao.OracleDao;


public class StoreToOracle {

	String[] infolist = {"AF_ESP","AF_EXAC","AF_TGP","ALLELEID","CLNDN","CLNDNINCL","CLNDISDB","CLNDISDBINCL",
    		"CLNHGVS","CLNREVSTAT","CLNSIG","CLNSIGCONF","CLNSIGINCL","CLNVC","CLNVCSO","CLNVI",
    		"DBVARID","GENEINFO","MC","ORIGIN","RS","SSR"};
	String[] data = {"CHROM","POS","ID","REF","ALT","QUAL","FILTER"};
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long t1 = new Date().getTime();
		new StoreToOracle().store(args[0],args[1]);
//		List<String> rs = new StoreToOracle().query(args[0],args[1]);
//		for(String s:rs){
//			System.out.println(s);
//		}
		long t2 = new Date().getTime();
		System.out.println("StoreToOracle 运行时间为："+(t2-t1)/1000 + "s");
//		System.out.println("QueryFromOracle 运行时间为："+(t2-t1)/1000 + "s");
	}

	public void store(String filePath,String tablename){
		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		
		PreparedStatement pstmt = null;
        Connection conn = null;
        String sql = "insert into "+tablename+"(CHROM,POS,ID,REF,ALT,QUAL,FILTER,"
        		+ "AF_ESP,AF_EXAC,AF_TGP,ALLELEID,CLNDN,CLNDNINCL,CLNDISDB,CLNDISDBINCL,"
        		+ "CLNHGVS,CLNREVSTAT,CLNSIG,CLNSIGCONF,CLNSIGINCL,CLNVC,CLNVCSO,CLNVI,"
        		+ "DBVARID,GENEINFO,MC,ORIGIN,RS,SSR) "
        		+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
		try {
			conn = OracleDao.getConnection();
			pstmt = conn.prepareStatement(sql);
			
			fis = new FileInputStream(filePath);
	        // 防止路径乱码   如果utf-8 乱码  改GBK     eclipse里创建的txt  用UTF-8，在电脑上自己创建的txt  用GBK
	        isr = new InputStreamReader(fis, "UTF-8");
	        br = new BufferedReader(isr);
	        String line = "";
			while ((line = br.readLine()) != null) {
			    if(line.startsWith("#")){
			    	continue;
			    }
			    String[] row = line.split("\\t");
			    pstmt.setString(1,row[0]);
			    pstmt.setInt(2, Integer.parseInt(row[1]));
			    pstmt.setString(3, row[2]);
			    pstmt.setString(4, row[3]);
			    pstmt.setString(5, row[4]);
			    if(!row[5].equals("."))
			    	pstmt.setString(6, row[5]);
			    else
			    	pstmt.setString(6, null);
			    if(!row[6].equals("."))
			    	pstmt.setString(7, row[6]);
			    else
			    	pstmt.setString(7, null);
			    String[] info = row[7].split(";");
			    Map<String,String> map = new HashMap<String,String>();
			    for(String detail:info){
			    	String[] p = detail.split("="); 
			    	map.put(p[0], p[1]);
			    }
			    for(int i=0;i<infolist.length;i++){
			    	pstmt.setString(8+i, map.get(infolist[i]));
			    }
			    pstmt.executeUpdate();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			try {
				if(br!=null) {
					br.close();
				}
				if(isr!=null) {
					isr.close();
				}
				if(fis!=null) {
					fis.close();
				}
				OracleDao.closeAll(conn, pstmt, null);
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
	}

	public List<String> query(String q,String tablename){
		List<String> list = new ArrayList<String>();
		Statement stmt = null;
        Connection conn = null;
        ResultSet rs = null;
        StringBuffer sql = new StringBuffer("select * from "+tablename+" where 1<>1 ");
        for(String s : infolist){
        	sql.append("OR "+s+" LIKE \'%"+q+"%\' ");
        }
        try {
			conn = OracleDao.getConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql.toString());
			while(rs.next()){
				StringBuffer row = new StringBuffer("{");
				for(int i=0;i<data.length;i++){
					String tmp = rs.getString(i+1);
					if(tmp!=null && !tmp.equals("")){
						row.append("\""+data[i]+"\":\""+tmp+"\",");
					}
				}
				row.append("\"info\":{");
				for(int i=0;i<infolist.length;i++){
					String tmp = rs.getString(i+data.length+1);
					if(tmp!=null && !tmp.equals("")){
						row.append("\""+infolist[i]+"\":\""+tmp+"\",");
					}
				}
				list.add(row.substring(0,row.length()-1)+"}}");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				OracleDao.closeAll(conn, stmt, rs);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
        return list;
	}
}
