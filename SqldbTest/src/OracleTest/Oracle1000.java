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

import dao.OracleDao;

public class Oracle1000 {
	List<String> infolist = new ArrayList<String>();
	List<String> samples = null;
	String[] data = {"CHROM","POS","ID","REF","ALT","QUAL","FILTER"};
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long t1 = new Date().getTime();
		new Oracle1000().store(args[0],args[1],Integer.parseInt(args[2]));//输入文件路径
//		new Oracle1000().store("E:\\学习\\研究生\\千人基因组数据\\ALL.chr21.phase3_shapeit2_mvncall_integrated_v2.20130502.genotypes.vcf","vcf1000");//输入文件路径
//		List<String> rs = new Oracle1000().query(args[0],args[1]);
//		List<String> rs = new Oracle1000().query("0.00138067","vcf1000");
//		for(String s:rs){
//			System.out.println(s);
//		}
		long t2 = new Date().getTime();
		System.out.println("StoreToOracle 运行时间为："+(t2-t1)/1000 + "s");
//		System.out.println("get total rs:"+rs.size());
//		System.out.println("QueryFromOracle 运行时间为："+(t2-t1) + "ms");
	}

	public void store(String filePath,String tablename, int colNum){
		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		
		PreparedStatement pstmt = null;
		PreparedStatement cpstmt = null;
        Connection conn = null;
        
        StringBuffer createSql = new StringBuffer(getCreateSqlPre(tablename));
        StringBuffer sql = new StringBuffer("insert into "+tablename+"(CHROM,POS,ID,REF,ALT,QUAL,FILTER");
//        		+ "AF_ESP,AF_EXAC,AF_TGP,ALLELEID,CLNDN,CLNDNINCL,CLNDISDB,CLNDISDBINCL,"
//        		+ "CLNHGVS,CLNREVSTAT,CLNSIG,CLNSIGCONF,CLNSIGINCL,CLNVC,CLNVCSO,CLNVI,"
//        		+ "DBVARID,GENEINFO,MC,ORIGIN,RS,SSR) "
//        		+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
		try {
			conn = OracleDao.getConnection();
//			pstmt = conn.prepareStatement(sql);
			
			fis = new FileInputStream(filePath);
	        // 防止路径乱码   如果utf-8 乱码  改GBK     eclipse里创建的txt  用UTF-8，在电脑上自己创建的txt  用GBK
	        isr = new InputStreamReader(fis, "UTF-8");
	        br = new BufferedReader(isr);
	        String line = "";
			while ((line = br.readLine()) != null) {
			    if(line.startsWith("#")){
			    	if(line.startsWith("##INFO")){
			    		String tmp = line.split(",")[0];
			    		String tag = tmp.substring(tmp.indexOf("ID")+3, tmp.length());
			    		if(!infolist.contains(tag)){
			    			infolist.add(tag);
			    		}
			    	}else if(line.startsWith("#CHROM")){
			    		//获取sample
			    		String[] tmp = line.split("\t");
			    		samples = new ArrayList<String>();
			    		for(int i=9;i<tmp.length&&(i+infolist.size())<=colNum;i++){
			    			if(i==2543){
			    				System.out.println(1);
			    			}
			    			samples.add(tmp[i]);
			    		}
			    		//组合sql语句
			    		for(String s:infolist){
			    			sql.append(","+s);
			    			createSql.append(","+s+" varchar2(128) DEFAULT NULL");
			    		}
			    		if(samples.size()>0){
			    			sql.append(",format");
			    			createSql.append(",FORMAT varchar2(128) DEFAULT NULL");
				    		for(String s:samples){
				    			sql.append(","+s);
				    			createSql.append(","+s+" varchar2(256) DEFAULT NULL ");
				    		}
			    		}
			    		createSql.append(")tablespace big01");
			    		sql.append(") values(");
			    		for(int i=0;i<(data.length+infolist.size()+samples.size()+1);i++){
			    			sql.append("?,");
			    		}
//			    		System.out.println(createSql);
			    		cpstmt = conn.prepareStatement(createSql.toString());
			    		cpstmt.executeUpdate();
			    		cpstmt.close();
			    		pstmt = conn.prepareStatement(sql.toString());
			    		pstmt = conn.prepareStatement(sql.substring(0,sql.length()-1)+")");
			    	}
			    	continue;
			    }
			    String[] row = line.split("\\t");
			    for(int i=0;i<data.length;i++){
			    	if(!row[i].equals("."))
			    		pstmt.setString(i+1,row[i]);
			    	else
			    		pstmt.setString(i+1,null);
			    }
			    //info注入
			    String[] info = row[7].split(";");
			    Map<String,String> map = new HashMap<String,String>();
			    for(String detail:info){
			    	String[] p = detail.split("="); 
			    	if(p!=null && p.length>=2)
			    		map.put(p[0], p[1]);
			    }
			    for(int i=0;i<infolist.size();i++){
			    	pstmt.setString(8+i, map.get(infolist.get(i)));
			    }
			    pstmt.setString(8+infolist.size(), row[8]);
			    //sample注
			    if(samples.size()>0){
			    	for(int i=0;i<samples.size();i++){
				    	pstmt.setString(9+infolist.size()+i, row[i+9]);
				    }
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

	private String getCreateSqlPre(String tablename) {
		return "CREATE TABLE "+tablename+"("+
				"CHROM varchar2(16) DEFAULT NULL,"
				+ "POS NUMBER(20) DEFAULT NULL,"
				+ "ID varchar2(64) DEFAULT NULL,"
				+ "REF CLOB,"
				+ "ALT CLOB,"
				+ "QUAL varchar2(32) DEFAULT NULL,"
				+ "FILTER varchar2(32) DEFAULT NULL";
	}

	public List<String> query(String q,String tablename){
		//加载info列
		infolist = loadinfo();
		
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
				StringBuffer infoData = new StringBuffer("\"info\":{");
				for(int i=0;i<infolist.size();i++){
					String tmp = rs.getString(i+data.length+1);
					if(tmp!=null && !tmp.equals("")){
						infoData.append("\""+infolist.get(i)+"\":\""+tmp+"\",");
					}
				}
				row.append(infoData.subSequence(0, infoData.length()-1)+"}");
				
				String tmp = rs.getString(data.length+infolist.size()+1);
				if(tmp!=null && !tmp.equals("")){
					row.append(",\"format\":\""+tmp+"\"");
				}
				tmp = rs.getString(data.length+infolist.size()+2);
				if(tmp!=null && !tmp.equals("")){
					row.append(",\"samples\":\""+tmp+"\"");
				}
				
				list.add(row.toString()+"}");
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

	private List<String> loadinfo() {
		List<String> list = new ArrayList<String>();
//		list.add("CIEND");list.add("CIPOS");list.add("END");
//		list.add("IMPRECISE");list.add("SVLEN");list.add("SVTYPE");
//		list.add("AC");list.add("AF");list.add("NS");
//		list.add("AN");
		
		list.add("AA");list.add("AC");list.add("AF");list.add("NS");list.add("AN");
		list.add("SAS_AF");list.add("EUR_AF");list.add("AFR_AF");list.add("AMR_AF");list.add("EAS_AF");
		list.add("VT");list.add("EX_TARGET");list.add("MULTI_ALLELIC");list.add("DP");list.add("END");
		list.add("SVTYPE");
		return list;
	}
}
