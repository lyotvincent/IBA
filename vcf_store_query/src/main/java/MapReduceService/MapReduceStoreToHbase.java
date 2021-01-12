package MapReduceService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class MapReduceStoreToHbase {
	
	static int colNum;
	static String docId;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long t1 = new Date().getTime();
		new MapReduceStoreToHbase().file2Hbase(args[0], args[1],args[2],Integer.parseInt(args[3]));
		long t2 = new Date().getTime();
		System.out.println("MapRed2hbase cost time "+(t2-t1)/1000+"s");
	}
	
	public void file2Hbase(String filename, String tablename,String docId,int colnum) {
		this.colNum = colnum;
		this.docId = docId;
		  Configuration conf = new Configuration();
		  //分布式配置
//		  conf.set("fs.defaultFS", "hdfs://mad:9000/");
//		  conf.set("hbase.zookeeper.quorum", "mad");
//		  conf.set("hbase.rootdir","hdfs://mad:9000/hbase");
		  conf.set("hbase.zookeeper.quorum", "node2.hadoop:2181,node3.hadoop:2181,node4.hadoop:2181");
		  conf.set("hbase.rootdir","hdfs://node1.hadoop:9000/hbase");
		  try {
			  Job job = Job.getInstance(conf, "md2hbase");
			  job.setJarByClass(MapReduceStoreToHbase.class);
			  job.setMapperClass(MapperClass.class);
			  job.setNumReduceTasks(0);
			  job.setOutputFormatClass(TableOutputFormat.class);
			  job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tablename);
			  FileInputFormat.addInputPath(job, new Path(filename));
			  System.out.println(job.waitForCompletion(true) ? 0 : 1);
			 } catch (ClassNotFoundException e) {
				e.printStackTrace();
			 } catch (InterruptedException e) {
				e.printStackTrace();
		    }catch (IOException e) {
		    	e.printStackTrace();
		}
	}

	public static class MapperClass extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
			
			//VCF标题行中的前8个固定列及必出现的可选列的列名
//		String[] COLUMNS = ["CHROM","POS","ID","REF","ALT","QUAL","FILTER","INFO"];
		List<String> COLUMNS = new ArrayList<String>();
//		String[] COLFAMILIES = {"data","info"};
		String[] COLFAMILIES = {"data","info","samples"};
//		private static int id = 1;
		//记录列限定符的个数，初始默认为10
		private static int columnNum = 10;
		private static int choiceNumber = 2;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			//初始化VCF标题行中的前8个固定列及必出现的可选列的列名
			COLUMNS.add("CHROM");
			COLUMNS.add("POS");
			COLUMNS.add("ID");
			COLUMNS.add("REF");
			COLUMNS.add("ALT");
			COLUMNS.add("QUAL");
			COLUMNS.add("FILTER");
			COLUMNS.add("INFO");
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if(value.toString().startsWith("#")) {
				return;
			}
			
			String choiceColumn = "NA0000";
			String[] columnVals = value.toString().split("\\t");
			//生成rowkey
			String rowkey = columnVals[0]+columnVals[1]+docId;
//			String rowkey = id + columnVals[0] + columnVals[1];
//			id ++;
			Put put = new Put(rowkey.getBytes());
			//若该行列数超过hbase表中列族中的列的个数，则需要动态扩展列
			if(columnVals.length > columnNum) {
				int diff = columnVals.length - columnNum;
				for(int i=0;i<diff;i++) {
					COLUMNS.add(choiceColumn + choiceNumber);
					choiceNumber ++;
				}
			}
			
			//除INFO列的其他列组成一个列族
			for(int i=0;i<7;i++) {
				put.addColumn(Bytes.toBytes(COLFAMILIES[0]), Bytes.toBytes(COLUMNS.get(i)), Bytes.toBytes(columnVals[i]));
			}
			//单独处理INFO列,将其单独作为一个列族，其值中的键值对分别作为列限定符和列值
			String[] infoColumn = columnVals[7].split(";");
			String col = "";
			String colVal = "";
			for(int i=0;i<infoColumn.length;i++) {
				String[] splitColumn = infoColumn[i].split("=");
				col = splitColumn[0];
				if(splitColumn.length == 1) {
					colVal = "";
				} else {
					colVal = splitColumn[1];
				}
				put.addColumn(Bytes.toBytes(COLFAMILIES[1]), Bytes.toBytes(col), Bytes.toBytes(colVal));
			}
			//加入samples
			put.addColumn(COLFAMILIES[2].getBytes(), "FORMAT".getBytes(), columnVals[8].getBytes());
			for(int i=9;i<columnVals.length && (i+infoColumn.length)<colNum;i++) {
					put.addColumn(Bytes.toBytes(COLFAMILIES[2]), Bytes.toBytes(COLUMNS.get(i)), Bytes.toBytes(columnVals[i]));
			}
			
			context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put);
		 }
	}
}
