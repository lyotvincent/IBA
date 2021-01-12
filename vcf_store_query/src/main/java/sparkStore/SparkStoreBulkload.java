package sparkStore;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HRegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class SparkStoreBulkload implements Serializable{

	private static final long serialVersionUID = 1563617037670911865L;
	String[] COLFAMILIES = {"data","info","sample"};
	 
    public static void main(String[] args) {
    	try {
    		long t1 = new Date().getTime();
//			new SparkStoreBulkload().saveHDFSHbaseHFile("hdfs://mad:9000/user/sjl/input/test2.vcf",
//					"vcftest",1,20);
    		new SparkStoreBulkload().saveHDFSHbaseHFile(args[0],
					args[1],Integer.parseInt(args[2]),Integer.parseInt(args[3]),args[4]);
    		long t2 = new Date().getTime();
    		System.out.println("Spark2hbase cost time "+(t2-t1)/1000+"s");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
   public  void saveHDFSHbaseHFile(String datasourcePath,String tablename,final int docId, final int colNum,String part) throws Exception { // 数据集的字段列表
	   SparkConf sparkConf = new SparkConf().setAppName("Spark2HBasebulk")
				.set("spark.scheduler.mode", "FAIR")
				.set("spark.default.parallelism", part)
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.set("spark.kryo.registrator","sparkStore.MyKryoRegister");
	   
	   JavaSparkContext sc = new JavaSparkContext(sparkConf);
	   sc.addJar("~/jarfiles/sparkjar/vcf-1.0.jar");
	   
	   Configuration hbaseConf = HBaseConfiguration.create();
       hbaseConf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 1024);
//       hbaseConf.setInt("hbase.bulkload.retries.number", 0);
       hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tablename);
       Connection conn = ConnectionFactory.createConnection(hbaseConf);
       Admin admin = conn.getAdmin();
       TableName tableName = TableName.valueOf(tablename);
        
        Job job = Job.getInstance();
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);
 
        
        // 0. 准备程序运行的环境
        // 如果 HBase 表不存在，就创建一个新表
        if (!admin.tableExists(tableName)) {
        	HTableDescriptor desc = new HTableDescriptor(tableName);
        	for(String cf :COLFAMILIES){
        		HColumnDescriptor hcd = new HColumnDescriptor(cf);
        		desc.addFamily(hcd);
        	}
            admin.createTable(desc);
        }
        // 如果存放 HFile文件的路径已经存在，就删除掉
        Configuration hadoopConf = new Configuration();
        FileSystem fileSystem = FileSystem.get(hadoopConf);
        String temp = "hdfs://node1.hadoop:9000/tmp/bulkload/"+tableName;
        if(fileSystem.exists(new Path(temp))) {
        	fileSystem.delete(new Path(temp), true);
        }
        
        HRegionLocator regionLocator = new HRegionLocator(tableName, (ClusterConnection) conn);
        Table realTable = ((ClusterConnection) conn).getTable(tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, realTable, regionLocator);
 
        JavaRDD<String> textfile = sc.textFile(datasourcePath);
        //获取元信息
		List<String> anno = textfile.filter(new Function<String,Boolean>(){
			private static final long serialVersionUID = 4676397532814333979L;
			public Boolean call(String line) throws Exception {
				if(line.startsWith("#"))
					return true;
				return false;
			}
			
		}).collect();
		//设置广播变量
		ArrayList<Tuple2<String,Integer>> datalist = new ArrayList<Tuple2<String, Integer>>();
		ArrayList<String> infolist = new ArrayList<String>();
		ArrayList<Tuple2<String,Integer>> samplelist = new ArrayList<Tuple2<String, Integer>>();
		for(String s : anno){
			if(s.startsWith("##INFO")){
	    		String tmp = s.split(",")[0];
	    		String tag = tmp.substring(tmp.indexOf("ID")+3, tmp.length());
	    		if(!infolist.contains(tag)){
	    			infolist.add(tag);
	    		}
	    	}else if(s.startsWith("#CHROM")){
				String[] tmp = s.split("\t");
				datalist.add(new Tuple2<String,Integer>(tmp[0].substring(1),0));
				for(int i=1;i<7;i++){
					datalist.add(new Tuple2<String,Integer>(tmp[i],i));
				}
				for(int i=8;i<tmp.length 
						&& (samplelist.size()+datalist.size()+infolist.size())<colNum;i++){
					samplelist.add(new Tuple2<String, Integer>(tmp[i],i));
				}
			}
		}
		// sort columns。这里需要对列进行排序，不然会报错
		Collections.sort(datalist,new Comparator<Tuple2<String, Integer>>() {
        	public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
        		// TODO Auto-generated method stub
				return o1._1.compareTo(o2._1);
			}
        });
        
        Collections.sort(infolist);
        Collections.sort(samplelist,new Comparator<Tuple2<String, Integer>>() {
        	public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
        		// TODO Auto-generated method stub
        		return o1._1.compareTo(o2._1);
        	}
        });
		final Broadcast<ArrayList<Tuple2<String, Integer>>> datas = sc.broadcast(datalist);
//		System.out.println(datalist.toString());
		final Broadcast<ArrayList<String>> infos = sc.broadcast(infolist);
//		System.out.println(infolist.toString());
		final Broadcast<ArrayList<Tuple2<String, Integer>>> samples = sc.broadcast(samplelist);
//		System.out.println(samplelist.toString());
 
		JavaRDD<String> javaRDD = textfile.filter(new Function<String,Boolean>(){
			private static final long serialVersionUID = 3911747218220824678L;

			public Boolean call(String line) throws Exception {
				// TODO Auto-generated method stub
				return !line.startsWith("#");
			}
		});
        JavaPairRDD<ImmutableBytesWritable, KeyValue> javaPairRDD =
                javaRDD.mapToPair(new PairFunction<String, ImmutableBytesWritable, List<Tuple2<ImmutableBytesWritable, KeyValue>>>() {
					private static final long serialVersionUID = 2063250436760623971L;

			public Tuple2<ImmutableBytesWritable, List<Tuple2<ImmutableBytesWritable, KeyValue>>> call(String line) throws Exception {
                List<Tuple2<ImmutableBytesWritable, KeyValue>> tps = new ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>>();
                ArrayList<Tuple2<String, Integer>> dataList = datas.value();
                ArrayList<String> infoList = infos.value();
                ArrayList<Tuple2<String, Integer>> sampleList = samples.value();
                
                //生成rowkey
                String[] row = line.split("\t");
                String rowkey = row[0]+row[1]+docId;
                ImmutableBytesWritable writable = new ImmutableBytesWritable(Bytes.toBytes(rowkey));
 
                //取值
                Map<String,String> infomap = new HashMap<String,String>();
                
                String[] info = row[7].split(";");
        		for(String detail:info){
			    	String[] p = detail.split("="); 
			    	if(p!=null && p.length>=2)
			    		infomap.put(p[0], p[1]);
			    }
			    for(int i =0;i<dataList.size();i++){
			    	String value = row[dataList.get(i)._2];
			    	if(value!=null && !value.equals(".")){
			    		KeyValue kv = new KeyValue(Bytes.toBytes(rowkey),
	                            Bytes.toBytes(COLFAMILIES[0]),
	                            Bytes.toBytes(dataList.get(i)._1), Bytes.toBytes(value));
	                    tps.add(new Tuple2<ImmutableBytesWritable, KeyValue>(writable, kv));
			    	}
			    }
			    for(int i =0;i<infoList.size();i++){
			    	String value = infomap.get(infoList.get(i));
			    	if(value!=null && !value.equals(".")){
	            		KeyValue kv = new KeyValue(Bytes.toBytes(rowkey),
	                            Bytes.toBytes(COLFAMILIES[1]),
	                            Bytes.toBytes(infoList.get(i)), Bytes.toBytes(value));
	                    tps.add(new Tuple2<ImmutableBytesWritable, KeyValue>(writable, kv));
			    	}
			    }
			    for(int i =0;i<sampleList.size();i++){
			    	String value = row[sampleList.get(i)._2];
			    	if(value!=null && !value.equals(".")){
	            		KeyValue kv = new KeyValue(Bytes.toBytes(rowkey),
	                            Bytes.toBytes(COLFAMILIES[2]),
	                            Bytes.toBytes(sampleList.get(i)._1), Bytes.toBytes(value));
	                    tps.add(new Tuple2<ImmutableBytesWritable, KeyValue>(writable, kv));
			    	}
			    }
			    
                return new Tuple2<ImmutableBytesWritable, List<Tuple2<ImmutableBytesWritable, KeyValue>>>(writable, tps);
            }
       // 这里一定要按照rowkey进行排序，这个效率很低，目前没有找到优化的替代方案
        }).reduceByKey(new Function2<List<Tuple2<ImmutableBytesWritable, KeyValue>>,List<Tuple2<ImmutableBytesWritable, KeyValue>>,
        		List<Tuple2<ImmutableBytesWritable, KeyValue>>>(){
					private static final long serialVersionUID = -134763418591688822L;

					public List<Tuple2<ImmutableBytesWritable, KeyValue>> call(
							List<Tuple2<ImmutableBytesWritable, KeyValue>> kv1,
							List<Tuple2<ImmutableBytesWritable, KeyValue>> kv2) throws Exception {
						// TODO Auto-generated method stub
						return kv1;
					}
        	
        }).sortByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, List<Tuple2<ImmutableBytesWritable, KeyValue>>>,
                ImmutableBytesWritable, KeyValue>() {
					private static final long serialVersionUID = 1254388259270824538L;

			public Iterator<Tuple2<ImmutableBytesWritable, KeyValue>> call(Tuple2<ImmutableBytesWritable,
                    List<Tuple2<ImmutableBytesWritable, KeyValue>>> tuple2s) throws Exception {
//				Iterator it =tuple2s._2().iterator();
//				while(it.hasNext()) {  
//				    System.out.println(it.next().toString());  
//				}  
                return tuple2s._2().iterator();
            }
        });
 
        // 创建HDFS的临时HFile文件目录
        javaPairRDD.saveAsNewAPIHadoopFile(temp, ImmutableBytesWritable.class,
                KeyValue.class, HFileOutputFormat2.class, job.getConfiguration());
 
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConf);
        loader.doBulkLoad(new Path(temp), admin, realTable, regionLocator);
 
    }

}
