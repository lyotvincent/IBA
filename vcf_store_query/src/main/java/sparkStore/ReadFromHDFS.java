package sparkStore;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

//import scala.Tuple2;

public class ReadFromHDFS implements Serializable{
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1786656777915270562L;
	String[] COLUMNS = {"CHROM","POS","ID","REF","ALT","QUAL","FILTER","INFO"};
	String[] COLFAMILIES = {"data","info"};
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ReadFromHDFS rh = new ReadFromHDFS();
		rh.read("hdfs://mad:9000/user/sjl/input/test1.vcf","test");
	}
	
	public void read(String path,final String tableName){//.setMaster("spark://sjl-ubuntu:7077")
		SparkConf sparkConf = new SparkConf().setAppName("read_from_hdfs_test")
				.set("spark.scheduler.mode", "FAIR");
//				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//				.set("spark.kryo.registrator","sparkStore.MyKryoRegister");
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.addJar("/home/sjl/Desktop/susu/vcf/target/vcf-1.0.jar");
//		final String tableName = "test";
		JavaRDD<String> textFile = sc.textFile(path);
		textFile.foreachPartition(new VoidFunction<Iterator<String>>(){
			
			/**
			 * 
			 */
			private static final long serialVersionUID = -2362734604644563137L;

			public void call(Iterator<String> lines) throws Exception {
				// TODO Auto-generated method stub
				Table tbl = getTable(tableName);
				while(lines.hasNext()){
					String line = lines.next();
					if(line.startsWith("#"))
						continue;
					String[] row = lines.next().split("\\t");
					String rowKey = row[0]+"_"+row[1]+"_"+row[3]+"_"+row[4];
					//插入data
					for(int i=0;i<row.length-1;i++){
						if(row[i].equals("."))
							continue;
						Put put = new Put(rowKey.getBytes());
						put.addColumn(COLFAMILIES[0].getBytes(), COLUMNS[i].getBytes(), row[i].getBytes());
						tbl.put(put);
					}
					//插入info
					String[] infoData = row[row.length-1].split(";");
					for(String info: infoData){
						String[] col = info.split("=");
						Put put = new Put(rowKey.getBytes());
						put.addColumn(COLFAMILIES[1].getBytes(), col[0].getBytes(), col[1].getBytes());
						tbl.put(put);
					}
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
