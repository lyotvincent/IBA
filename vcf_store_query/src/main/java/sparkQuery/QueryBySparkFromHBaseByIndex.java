package sparkQuery;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import dao.HbaseDao;
import model.Row;


/**
 * 已知omimID查询INFO(有索引)
 * @author ssh
 *
 */
public class QueryBySparkFromHBaseByIndex {
	
	String[] COLFAMILIES = {"data","info","sample"};
//	List<String> logic = new ArrayList<String>();
	List<List<String>> cf = new ArrayList<List<String>>();
	List<List<String>> col = new ArrayList<List<String>>();
	List<List<String>> comparator = new ArrayList<List<String>>();
	List<List<String>> values = new ArrayList<List<String>>();
	
	List<List<String>> indexcol = new ArrayList<List<String>>();
	List<List<String>> indexvalues = new ArrayList<List<String>>();
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		QueryBySparkFromHBaseByIndex qs = new QueryBySparkFromHBaseByIndex();
		qs.parseQuery(args[0]);
//		qs.parseQuery("data:ALT=A&info:AN=5000|sample:FORMAT=GT&info:AF=0.778698");
		long t1 = new Date().getTime();
		List<String> rs = qs.queryByIndex(args[1],args[2]);
//		List<String> rs = qs.queryByIndex("vcftest","");
//		for(String s:rs) {
//			System.out.println(s);
//		}
		System.out.println("total rs:"+rs.size());
		long t2 = new Date().getTime();
		System.out.println("QureyByIndex cost time "+(t2-t1)/1000+"s");
	}

	public List<String> queryByIndex(String tableName,String indexPath){
		List<String> rs = new ArrayList<String>();
		//打开存储位置
		Directory directory = null;
		DirectoryReader ireader = null;
		
		//通过id从HBASE中取出row
		HbaseDao dao = new HbaseDao();
		HbaseDao.init();
		Connection conn = dao.getConnection();
		Table table=null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			//从本地获取索引
//			directory = FSDirectory.open(Paths.get(indexPath));
			//从hdfs获取索引
			Configuration conf = new Configuration();
			directory = new HdfsDirectory(new Path(indexPath), conf);
			
			//读取索引
			ireader = DirectoryReader.open(directory);
			IndexSearcher isearcher = new IndexSearcher(ireader);
			
			Analyzer analyzer = new StandardAnalyzer();
			for(int i=0;i<indexcol.size();i++){
				Set<String> pqKey = new HashSet<String>();
				List<String> pqindexcol = indexcol.get(i);
				for(int j=0;j<pqindexcol.size();j++){
					QueryParser parser = new QueryParser(pqindexcol.get(j), analyzer);
					Query query = parser.parse(indexvalues.get(i).get(j));
					
					TopDocs hits = isearcher.search(query, 10000);
					ScoreDoc[] docs = hits.scoreDocs;
					if(pqKey.size()==0){
						for(ScoreDoc doc:docs){
							Document hitDoc = isearcher.doc(doc.doc);
							pqKey.add(hitDoc.get("rowkey"));
						}
					}else{
						Set<String> temp = new HashSet<String>();
						for(ScoreDoc doc:docs){
							Document hitDoc = isearcher.doc(doc.doc);
							temp.add(hitDoc.get("rowkey"));
						}
						pqKey.retainAll(temp);
					}
				}
				System.out.println("total rs:"+pqKey.size());
				//进行其他条件的对比
				for(String id :pqKey){
					Get get = new Get(id.getBytes());
			        Result row = table.get(get);
			        Boolean flag = null;
					for(int k=0;k<cf.get(i).size();k++){
						boolean f = true;
						byte[]  v = row.getValue(cf.get(i).get(k).getBytes(), col.get(i).get(k).getBytes());
						if(v!=null){
							String fvalue = new String(v);
							if(comparator.get(i).get(k).equals(">")) {
								f = (Double.parseDouble(fvalue) > Double.parseDouble(values.get(i).get(k)));
							}
							if(comparator.get(i).get(k).equals(">=")) {
								f = (Double.parseDouble(fvalue) >= Double.parseDouble(values.get(i).get(k)));
							}
							if(comparator.get(i).get(k).equals("<")) {
								f = (Double.parseDouble(fvalue) < Double.parseDouble(values.get(i).get(k)));
							}
							if(comparator.get(i).get(k).equals("<=")) {
								f = (Double.parseDouble(fvalue) <= Double.parseDouble(values.get(i).get(k)));
							}
							if(comparator.get(i).get(k).equals("=")) {
								f = fvalue.contains(values.get(i).get(k));
							}
						}else{
							f = false;
						}
						if(flag==null){
							flag = f;
						}else{
							flag = flag && f;
						}
					}
					if(flag == null || flag)
						rs.add(convertToJason(row));
				}
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {//关闭查询器
			try {
				if(ireader!=null) {
					ireader.close();
				}
				if(directory!=null) {
					directory.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		HbaseDao.close();
		return rs;
	}
	
	private String convertToJason(Result rs) {
		StringBuffer rowinfo = new StringBuffer("{\"rowKey\":\""+new String(rs.getRow())+"\",");
		
		Map<byte[], byte[]> data = rs.getFamilyMap(COLFAMILIES[0].getBytes());
		StringBuffer dataDetail = new StringBuffer("\"data\":{");
		for(Entry<byte[], byte[]> e : data.entrySet()){
			dataDetail.append("\""+new String(e.getKey())+"\":\""+new String(e.getValue())+"\",");
		}
		rowinfo.append(dataDetail.subSequence(0, dataDetail.length()-1)+"},");
		
		Map<byte[], byte[]> info = rs.getFamilyMap(COLFAMILIES[1].getBytes());
		StringBuffer infoDetail = new StringBuffer("\"info\":{");
		for(Entry<byte[], byte[]> e : info.entrySet()){
			infoDetail.append("\""+new String(e.getKey())+"\":\""+new String(e.getValue())+"\",");
		}
		rowinfo.append(infoDetail.subSequence(0, infoDetail.length()-1)+"},");
		
		Map<byte[], byte[]> samples = rs.getFamilyMap(COLFAMILIES[2].getBytes());
		if(samples.size()>0){
			StringBuffer smpDetail = new StringBuffer("\"sample\":{");
			for(Entry<byte[], byte[]> e : samples.entrySet()){
				smpDetail.append("\""+new String(e.getKey())+"\":\""+new String(e.getValue())+"\",");
			}
			rowinfo.append(smpDetail.subSequence(0, smpDetail.length()-1)+"},");
		}
		return rowinfo.substring(0, rowinfo.length()-1)+"}";
	}

	private void parseQuery(String q) {
		List<String> qlist = new ArrayList<String>();
		int pre = 0;
		for(int i=0;i<q.length();i++) {
			if(q.charAt(i) == '|' 
					&& (q.charAt(i+1) == 'd' || q.charAt(i+1) == 'i' || q.charAt(i+1) == 's')) {//|后跟着的是列族名
				qlist.add(q.substring(pre, i));
				pre = i+1;
			}
		}
		qlist.add(q.substring(pre, q.length()));
		for(String s:qlist){
			List<String> pqcf = new ArrayList<String>();
			List<String> pqcol = new ArrayList<String>();
			List<String> pqcomparator = new ArrayList<String>();
			List<String> pqvalues = new ArrayList<String>();
			List<String> pqindexcol = new ArrayList<String>();
			List<String> pqindexvalues = new ArrayList<String>();
			
			String[] pq = s.split("&");
			for(String e : pq){
				String op = "";
				//提取语句中的比较符号
				for(int j=0;j<e.length();j++) {                                                         
					if(e.charAt(j) == '>' || e.charAt(j) == '<' || e.charAt(j) == '=' || e.charAt(j) == '!') {
					   op = op + e.charAt(j); 
					}
				}
				String[] splitS = e.split("[><=!]");
				String[] familycol = splitS[0].split(":");
				if(!familycol[0].equals("info")){
					//此处需要注意！带"<=",">="的语句如vcf:QUAL<=1000在拆分后形成的数组元素个数为3,并不是2.分别为"vcf:QUAL","","1000"
					//此时的值是数组第三个元素，若按第二个元素则值为空
					if(op.length() == 2) {
						pqvalues.add(splitS[2]);
					}else {
						pqvalues.add(splitS[1]);
					}
					pqcomparator.add(op);
					pqcf.add(familycol[0]);
					pqcol.add(familycol[1]);
				}else{
					pqindexcol.add(familycol[1]);
					pqindexvalues.add(splitS[1]);
				}
			}
			cf.add(pqcf);
			col.add(pqcol);
			comparator.add(pqcomparator);
			values.add(pqvalues);
			indexcol.add(pqindexcol);
			indexvalues.add(pqindexvalues);
		}
	}
}
