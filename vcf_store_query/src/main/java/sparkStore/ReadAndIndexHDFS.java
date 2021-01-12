package sparkStore;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
/**
 * 创建索引
 * 从hdfs中读取文件解析INFO部分列，建立索引存放在机器本地文件系统
 * @author ssh
 *
 */
public class ReadAndIndexHDFS implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5691943437351120609L;
	
	public static void main(String[] args) {
		long t1 = new Date().getTime();
		//存到本地
//		ri.readAndIndex("hdfs://mad:9000/user/sjl/input/test1.vcf",
//				"/home/sjl/Desktop/susu/index");
		//存到hdfs
		new ReadAndIndexHDFS().readAndIndex(args[0],args[1],Integer.parseInt(args[2]),args[3]);
		long t2 = new Date().getTime();
		System.out.println("SparkIndex cost time "+(t2-t1)/1000+"s");
	}
	
	public void readAndIndex(String path,final String indexPath,final int docId,String part){
		SparkConf sparkConf = new SparkConf().setAppName("SparkIndex")
				.set("spark.scheduler.mode", "FAIR")
				.set("spark.default.parallelism", part)
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.set("spark.kryo.registrator","sparkStore.MyKryoRegister");
	    
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.addJar("~/jarfiles/sparkjar/vcf-1.0.jar");
			
		JavaRDD<String> textFile = sc.textFile(path);
		
		textFile = textFile.filter(new Function<String,Boolean>(){

			private static final long serialVersionUID = 3911747218220824678L;

			public Boolean call(String line) throws Exception {
				// TODO Auto-generated method stub
				return !line.startsWith("#");
			}
		});
		
		JavaRDD<Document> lrdd	=textFile.map(new Function<String,Document>(){

			private static final long serialVersionUID = -7431164115743970176L;

			public Document call(String line) throws Exception {
				String[] row = line.split("\\t");
				//生成rowkey
				String rowKey = row[0]+row[1]+docId;
				//插入info并创建索引
				//内容提取，进行索引的存储。
                Document doc = new Document();
				doc.add(new TextField("rowkey", rowKey,Store.YES));
				String[] infoData = row[7].split(";");
				for(String info: infoData){
					String[] col = info.split("=");
					//获取索引内容
					if(col!=null && col.length>=2){
						String[] colDetail = col[1].split(",");
						for(String str : colDetail){
							//冒号“:”不能作为索引内容
							doc.add(new TextField(col[0], str.replaceAll(":", "_"),Store.YES));
						}
					}
				}
				return doc;
			}
		});

		//存储索引
//		LoadIndexToLocal(lrdd.collect(),indexPath);//存到本地
		LoadIndexToHDFS(lrdd.collect(),indexPath);//存到HDFS
    }
	
	/**
	 * 利用Lucene和MapReduce将索引存进hdfs
	 * @param path
	 * @param tableName
	 * @param indexPath
	 */
	public void LoadIndexToHDFS(List<Document> indexDoc,String indexPath){
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		Configuration conf = new Configuration();
		HdfsDirectory directory;
		IndexWriter writer=null;
		try {
			FileSystem fs=FileSystem.get(conf);
	        Path p=new Path(indexPath);
	        if(fs.exists(p)){
	            fs.delete(p, true);
	            System.out.println("输出路径存在，已删除！");
	        }
			directory = new HdfsDirectory(p, conf);
			writer=new IndexWriter(directory, config);
			for(int i=0;i<indexDoc.size();i++){
				writer.addDocument(indexDoc.get(i));
				if(i%10000==0){
					writer.commit();
				}
			}
			writer.commit();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			closeWriter(writer);
		}
	}
	
	//存进本地
	public void LoadIndexToLocal(List<Document> indexDoc,String indexPath){
		IndexWriter indexWriter = null;
		try {
			indexWriter = getIndexWriter(indexPath);
			for(Document doc:indexDoc){
				indexWriter.addDocument(doc);
			}
			indexWriter.commit();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			closeWriter(indexWriter);
		}
	}

	public Table getTable(String tableName){
		Configuration conf = HBaseConfiguration.create();
		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		try {
			Connection conn = ConnectionFactory.createConnection(conf);
			return conn.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public IndexWriter getIndexWriter(String dir){
		Analyzer analyzer = new StandardAnalyzer();
		//创建IndexWriter，进行索引文件的写入
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        Directory directory;
		try {
			directory = FSDirectory.open(Paths.get(dir));
			 File indexFile = new File(dir);
             if (!indexFile.exists()) {
                 indexFile.mkdirs();
             }
			IndexWriter writer=new IndexWriter(directory,config); 
			return writer;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return null;
	}
	
	public void closeWriter(IndexWriter writer) {
    	try {
    		if (writer != null) {
    			writer.close();
    		}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }
	
}
