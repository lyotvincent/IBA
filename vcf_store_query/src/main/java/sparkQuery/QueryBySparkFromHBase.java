package sparkQuery;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

/**
 * 
 * 已知omimID查询INFO(利用filter，无索引)
 * @author Administrator
 *
 */
public class QueryBySparkFromHBase implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7593254027906118692L;
//	String[] COLFAMILIES = {"data","info"};
	String[] COLFAMILIES = {"data","info","sample"};
//	List<String> logic = new ArrayList<String>();
	List<List<String>> cf = new ArrayList<List<String>>();
	List<List<String>> col = new ArrayList<List<String>>();
	List<List<String>> comparator = new ArrayList<List<String>>();
	List<List<String>> values = new ArrayList<List<String>>();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		QueryBySparkFromHBase qs = new QueryBySparkFromHBase();
		qs.parseQuery(args[0]);
//		qs.parseQuery("data:ALT=A&info:AN=5000|sample:FORMAT=GT&info:AF=0.778698");
		long t1 = new Date().getTime();
//		List<String> rs = qs.query(args[1],args[2]);
		List<String> rs = qs.query("test",20+"");
//		for(String s:rs) {
//			System.out.println(s);
//		}
//		System.out.println("total rs:"+rs.size());
		long t2 = new Date().getTime();
		System.out.println("QueryBySpark cost time "+(t2-t1)/1000+"s");
	}
	
	

	public List<String> query(String tableName,String part){
		SparkConf sparkConf = new SparkConf().setAppName("queryBySpark")
				.set("spark.scheduler.mode", "FAIR")
				.set("spark.default.parallelism", part);
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.addJar("~/jarfiles/sparkjar/vcf-1.0.jar");
		
		Configuration conf = HBaseConfiguration.create();
		conf.set(TableInputFormat.INPUT_TABLE, tableName);
//		conf.set(TableInputFormat.SCAN_COLUMNS, "info:CLNDISDB info:CLNDISDBINCL");
		
		JavaPairRDD<ImmutableBytesWritable, Result> scanResult = sc.newAPIHadoopRDD(conf, TableInputFormat.class, 
				ImmutableBytesWritable.class, Result.class)
				.filter(new Function<Tuple2<ImmutableBytesWritable, Result>,Boolean>(){

					private static final long serialVersionUID = 1405630465118243439L;

					public Boolean call(Tuple2<ImmutableBytesWritable, Result> row) throws Exception {
						for(int i=0;i<cf.size();i++){
							Boolean flag = null;
							for(int j=0;j<cf.get(i).size();j++){
								boolean f = true;
								byte[]  v = row._2.getValue(cf.get(i).get(j).getBytes(), col.get(i).get(j).getBytes());
								if(v!=null){
									String fvalue = new String(v);
									if(comparator.get(i).get(j).equals(">")) {
										f = (Double.parseDouble(fvalue) > Double.parseDouble(values.get(i).get(j)));
									}
									if(comparator.get(i).get(j).equals(">=")) {
										f = (Double.parseDouble(fvalue) >= Double.parseDouble(values.get(i).get(j)));
									}
									if(comparator.get(i).get(j).equals("<")) {
										f = (Double.parseDouble(fvalue) < Double.parseDouble(values.get(i).get(j)));
									}
									if(comparator.get(i).get(j).equals("<=")) {
										f = (Double.parseDouble(fvalue) <= Double.parseDouble(values.get(i).get(j)));
									}
									if(comparator.get(i).get(j).equals("=")) {
										f = fvalue.contains(values.get(i).get(j));
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
							if(flag){
								return flag;
							}
						}
						return false;
					}
						
		});
		JavaRDD<String> rs = scanResult.map(new Function<Tuple2<ImmutableBytesWritable, Result>,String>(){
		
			private static final long serialVersionUID = -6870452984328502490L;

			public String call(Tuple2<ImmutableBytesWritable, Result> keyValue) throws Exception {
				Result rs = keyValue._2;
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
			
		});
		
		rs.saveAsTextFile("hdfs://node1.hadoop:9000/output/search.txt");
		return null;
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
			}
			cf.add(pqcf);
			col.add(pqcol);
			comparator.add(pqcomparator);
			values.add(pqvalues);
		}
		
	}
}
