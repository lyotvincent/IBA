package sparkStore;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

public class StoreVCFBySpark implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6093819640078868891L;
	String[] COLUMNS = {"CHROM","POS","ID","REF","ALT","QUAL","FILTER","INFO"};
//	String[] COLFAMILIES = {"data","info"};
	String[] COLFAMILIES = {"data","info","sample"};
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long t1 = new Date().getTime();
		//添加数据前需要到hbase dao或者shell创建表
		new StoreVCFBySpark().readAndStore(args[0],args[1],args[2],Integer.parseInt(args[3]),args[4]);
		long t2 = new Date().getTime();
		System.out.println("Spark2hbase cost time "+(t2-t1)/1000+"s");
	}
	
	//从hdfs中读取Clinvar数据，并插入到Hbase中（无索引）
		public void readAndStore(String path,final String tableName,String part,int colNum,final String docId){//.setMaster("spark://sjl-ubuntu:7077")
			SparkConf sparkConf = new SparkConf().setAppName("Spark2hbase")
					.set("spark.scheduler.mode", "FAIR")
					.set("spark.default.parallelism", part);
//					.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//					.set("spark.kryo.registrator","sparkStore.MyKryoRegister");
			
			JavaSparkContext sc = new JavaSparkContext(sparkConf);
			sc.addJar("~/jarfiles/sparkjar/vcf-1.0.jar");
//			final String tableName = "test";
			JavaRDD<String> textFile = sc.textFile(path);
			//获取元信息
			List<String> anno = textFile.filter(new Function<String,Boolean>(){

				private static final long serialVersionUID = 4676397532814333979L;

				public Boolean call(String line) throws Exception {
					if(line.startsWith("#"))
						return true;
					return false;
				}
				
			}).collect();
			//设置广播变量
			List<String> sampleslist = null;
			List<String> infolist = new ArrayList<String>();
			for(String s : anno){
				if(s.startsWith("##INFO")){
		    		String tmp = s.split(",")[0];
		    		String tag = tmp.substring(tmp.indexOf("ID")+3, tmp.length());
		    		if(!infolist.contains(tag)){
		    			infolist.add(tag);
		    		}
		    	}else if(s.startsWith("#CHROM")){
					sampleslist = new ArrayList<String>();
					String[] tmp = s.split("\t");
					for(int i=9;i<tmp.length&&(i+infolist.size())<=colNum;i++){
						sampleslist.add(tmp[i]);
					}
				}
			}
			final Broadcast<List<String>> samples = sc.broadcast(sampleslist);
			
			textFile.foreachPartition(new VoidFunction<Iterator<String>>(){
				/**
				 * 
				 */
				private static final long serialVersionUID = -5864651338245616346L;

				public void call(Iterator<String> lines) throws Exception {
					// TODO Auto-generated method stub
					Table tbl = getTable(tableName);
					for(Iterator<String> it = lines ;it.hasNext();){
						String line = it.next();
						if(line.startsWith("#")){
							continue;
						}
						List<String> sample = samples.value();
//						System.out.println(sample.size());	
						String[] row = line.split("\\t");
						//生成rowkey
//						String row_3 = row[3];
//						if(row_3.length()>10){
//							row_3 = row_3.substring(0, 10);
//						}
//						String row_4 = row[4];
//						if(row_4.length()>10){
//							row_4 = row_4.substring(0, 10);
//						}
						String rowKey = row[0]+row[1]+docId;
						Put put = new Put(rowKey.getBytes());
						//插入data
						for(int i=0;i<7;i++){
							if(row[i].equals("."))
								continue;
							put.addColumn(COLFAMILIES[0].getBytes(), COLUMNS[i].getBytes(), row[i].getBytes());
						}
						//插入info
						String[] infoData = row[7].split(";");
						for(String info: infoData){
							String[] col = info.split("=");
							if(col!=null && col.length>=2)
								put.addColumn(COLFAMILIES[1].getBytes(), col[0].getBytes(), col[1].getBytes());
						}
						//当存在samples信息，插入samples
						if(sample.size()>0){
							put.addColumn(COLFAMILIES[2].getBytes(), "FORMAT".getBytes(), row[8].getBytes());
							for(int i=0;i+9<row.length&&i<sample.size();i++){
								put.addColumn(COLFAMILIES[2].getBytes(), sample.get(i).getBytes(), row[i+9].getBytes());
							}
						}
//						put.addColumn(COLFAMILIES[2].getBytes(), "SAMPLE".getBytes(), sb.toString().getBytes());
						tbl.put(put);
					}
					tbl.close();
				}
			});
		}
		
		public Table getTable(String tableName){
			Configuration conf = HBaseConfiguration.create();
			conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
			try {
				Connection conn = ConnectionFactory.createConnection(conf);
				return conn.getTable(TableName.valueOf(tableName));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}

}
