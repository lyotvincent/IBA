package sparkQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class QuerySpark {
	
	List<String> logic = new ArrayList<String>();
	List<String> cf = new ArrayList<String>();
	List<String> col = new ArrayList<String>();
	List<String> comparator = new ArrayList<String>();
	List<String> values = new ArrayList<String>();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		QuerySpark qs = new QuerySpark();
		qs.parseQuery(args[0]);
//		qs.parseQuery("DATA:POS<=949502&INFO:CLNDISDB=OMIM:616126");
		long t1 = new Date().getTime();
		List<String> rs = qs.query(args[1],args[2]);
//		List<String> rs = qs.query("test");
//		for(String s:rs) {
//			System.out.println(s);
//		}
		System.out.println("total rs:"+rs.size());
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
		
		JavaPairRDD<ImmutableBytesWritable, Result> scanResult = sc.newAPIHadoopRDD(conf, TableInputFormat.class, 
				ImmutableBytesWritable.class, Result.class)
				.filter(new Function<Tuple2<ImmutableBytesWritable, Result>,Boolean>(){

					private static final long serialVersionUID = 1405630465118243439L;

					public Boolean call(Tuple2<ImmutableBytesWritable, Result> row) throws Exception {
						Boolean flag = null;
						for(int i=0;i<cf.size();i++){
							boolean f = true;
							byte[]  v = row._2.getValue(cf.get(i).getBytes(), col.get(i).getBytes());
							if(v!=null){
								String fvalue = new String(v);
								if(comparator.get(i).equals(">")) {
									f = (Double.parseDouble(fvalue) > Double.parseDouble(values.get(i)));
								}
								if(comparator.get(i).equals(">=")) {
									f = (Double.parseDouble(fvalue) >= Double.parseDouble(values.get(i)));
								}
								if(comparator.get(i).equals("<")) {
									f = (Double.parseDouble(fvalue) < Double.parseDouble(values.get(i)));
								}
								if(comparator.get(i).equals("<=")) {
									f = (Double.parseDouble(fvalue) <= Double.parseDouble(values.get(i)));
								}
								if(comparator.get(i).equals("=")) {
									f = fvalue.contains(values.get(i));
								}
							}else{
								f = false;
							}
							if(flag==null){
								flag = f;
							}else{
								if(logic.get(i-1).equals("&"))
									flag = flag && f;
								else if(logic.get(i-1).equals("|"))
									flag = flag || f;
							}
						}
						return flag;
					}
		});
		return null;
		
//			.filter(new Function<Tuple2<ImmutableBytesWritable, Result>,Boolean>(){
//	
//				private static final long serialVersionUID = 1405630465118243439L;
//	
//				public Boolean call(Tuple2<ImmutableBytesWritable, Result> row) throws Exception {
//					Boolean flag = null;
//					for(int i=0;i<cf.size();i++){
//						boolean f = true;
//						byte[]  v = row._2.getValue(cf.get(i).getBytes(), col.get(i).getBytes());
//						if(v!=null){
//							String fvalue = new String(v);
//							if(comparator.get(i).equals(">")) {
//								f = (Double.parseDouble(fvalue) > Double.parseDouble(values.get(i)));
//							}
//							if(comparator.get(i).equals(">=")) {
//								f = (Double.parseDouble(fvalue) >= Double.parseDouble(values.get(i)));
//							}
//							if(comparator.get(i).equals("<")) {
//								f = (Double.parseDouble(fvalue) < Double.parseDouble(values.get(i)));
//							}
//							if(comparator.get(i).equals("<=")) {
//								f = (Double.parseDouble(fvalue) <= Double.parseDouble(values.get(i)));
//							}
//							if(comparator.get(i).equals("=")) {
//								f = fvalue.contains(values.get(i));
//							}
//						}else{
//							f = false;
//						}
//						if(flag==null){
//							flag = f;
//						}else{
//							if(logic.get(i-1).equals("&"))
//								flag = flag && f;
//							else if(logic.get(i-1).equals("|"))
//								flag = flag || f;
//						}
//					}
//					return flag;
//				}
//	});
//	JavaRDD<String> rs = scanResult.map(new Function<Tuple2<ImmutableBytesWritable, Result>,String>(){
//	
//		private static final long serialVersionUID = -6870452984328502490L;
//	
//		public String call(Tuple2<ImmutableBytesWritable, Result> keyValue) throws Exception {
//			Result rs = keyValue._2;
//			StringBuffer rowinfo = new StringBuffer("{\"rowKey\":\""+new String(rs.getRow())+"\",");
//			
//			Map<byte[], byte[]> data = rs.getFamilyMap(COLFAMILIES[0].getBytes());
//			StringBuffer dataDetail = new StringBuffer("\"data\":{");
//			for(Entry<byte[], byte[]> e : data.entrySet()){
//				dataDetail.append("\""+new String(e.getKey())+"\":\""+new String(e.getValue())+"\",");
//			}
//			rowinfo.append(dataDetail.subSequence(0, dataDetail.length()-1)+"},");
//			
//			Map<byte[], byte[]> info = rs.getFamilyMap(COLFAMILIES[1].getBytes());
//			StringBuffer infoDetail = new StringBuffer("\"info\":{");
//			for(Entry<byte[], byte[]> e : info.entrySet()){
//				infoDetail.append("\""+new String(e.getKey())+"\":\""+new String(e.getValue())+"\",");
//			}
//			rowinfo.append(infoDetail.subSequence(0, infoDetail.length()-1)+"},");
//			
//			Map<byte[], byte[]> samples = rs.getFamilyMap(COLFAMILIES[2].getBytes());
//			if(samples.size()>0){
//				StringBuffer smpDetail = new StringBuffer("\"sample\":{");
//				for(Entry<byte[], byte[]> e : samples.entrySet()){
//					smpDetail.append("\""+new String(e.getKey())+"\":\""+new String(e.getValue())+"\",");
//				}
//				rowinfo.append(smpDetail.subSequence(0, smpDetail.length()-1)+"},");
//			}
//			
//			return rowinfo.substring(0, rowinfo.length()-1)+"}";
//		}
//		
//	});
//	
//	return rs.collect();
	}
	
	private void parseQuery(String q) {
		for(int i=0;i<q.length();i++) {
			if(q.charAt(i) == '&' || q.charAt(i) == '|') {
				logic.add(q.charAt(i)+"");
			}
		}
		String[] pq = q.split("[&|]");
		for(String e : pq){
			String op = "";
			//提取语句中的比较符号
			for(int j=0;j<e.length();j++) {                                                         
				if(e.charAt(j) == '>' || e.charAt(j) == '<' || e.charAt(j) == '=' || e.charAt(j) == '!') {
				   op = op + e.charAt(j); 
				}
			}
			comparator.add(op);
			String[] splitS = e.split("[><=!]");
			//此处需要注意！带"<=",">="的语句如vcf:QUAL<=1000在拆分后形成的数组元素个数为3,并不是2.分别为"vcf:QUAL","","1000"
			//此时的值是数组第三个元素，若按第二个元素则值为空
			if(op.length() == 2) {
				values.add(splitS[2]);
			}else {
				values.add(splitS[1]);
			}
			String[] familycol = splitS[0].split(":");
			cf.add(familycol[0]);
			col.add(familycol[1]);
		}
		
	}

}
