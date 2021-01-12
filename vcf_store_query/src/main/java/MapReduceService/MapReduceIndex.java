package MapReduceService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.apache.lucene.document.Field.Store;

/**
 * 利用MapReduce创建索引(需要修改getIndexWriter的文件路径)
 * @author Administrator
 *
 */
public class MapReduceIndex {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(args[0]);
		System.out.println(args[1]);
		long t1 = new Date().getTime();
		new MapReduceIndex().createIndex(args[0],args[1]);
		long t2 = new Date().getTime();
		System.out.println("mapRedIndex cost time "+(t2-t1)/1000+"s");
	}
	
	public void createIndex(String filename, String indexPath){
		Configuration conf = new Configuration();
		  //分布式配置
//		  conf.set("fs.defaultFS", "hdfs://node101:9006/");
//		  conf.set("hbase.zookeeper.quorum", "node2,node3,node4");
//		  conf.set("hbase.rootdir","hdfs://node101:9006/hbase");
		try {
			Job job = Job.getInstance(conf, "mdIndex");
			job.setJarByClass(MapReduceIndex.class);
			job.setMapperClass(MapperClass.class);
			job.setNumReduceTasks(0);
			job.setInputFormatClass(TextInputFormat.class);

			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			job.setOutputFormatClass(TextOutputFormat.class);
			
	        FileSystem fs=FileSystem.get(conf);
	        Path p=new Path(indexPath);
	        if(fs.exists(p)){
	            fs.delete(p, true);
	            System.out.println("输出路径存在，已删除！");
	        }
	        FileInputFormat.setInputPaths(job, filename);
	        FileOutputFormat.setOutputPath(job,p);
	        System.out.println(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
		    	e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static class MapperClass extends Mapper<LongWritable, Text, ImmutableBytesWritable, Document> {
		
			//VCF标题行中的前8个固定列及必出现的可选列的列名
	//	String[] COLUMNS = ["CHROM","POS","ID","REF","ALT","QUAL","FILTER","INFO"];
		String[] COLFAMILIES = {"data","info"};
		List<String> indexFeild = null;
		List<Document> documenst = new ArrayList<Document>();
		
//		private static int id = 1;
		//记录列限定符的个数，初始默认为10
		
		IndexWriter iw;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			//初始化VCF标题行中的前8个固定列及必出现的可选列的列名
			indexFeild = loadIndexField();
			
			Random rd = new Random();
	        int i = rd.nextInt(99999999);//此处的索引目录名可以使用UUID来使它唯一
	        try{
	        iw = getIndexWriter(i+"");//初始化IndexWriter
	        }catch(Exception e){
	            e.printStackTrace();
	        }
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if(value.toString().startsWith("#")) {
				return;
			}
			
			String[] columnVals = value.toString().split("\\t");
			//生成rowkey
			String row_3 = columnVals[3];
			if(row_3.length()>10){
				row_3 = row_3.substring(0, 10);
			}
			String row_4 = columnVals[4];
			if(row_4.length()>10){
				row_4 = row_4.substring(0, 10);
			}
			String rowkey = columnVals[0]+columnVals[1]+row_3+row_4;
//			String rowkey = id + columnVals[0] + columnVals[1];
//			id ++;
			Document doc = new Document();
			doc.add(new TextField("rowkey",rowkey,Store.YES));
			
			//单独处理INFO列,将其单独作为一个列族，其值中的键值对分别作为列限定符和列值
			String[] infoColumn = columnVals[7].split(";");
			for(String info: infoColumn){
				String[] col = info.split("=");
				//获取索引内容
				if(indexFeild.contains(col[0])){
					doc.add(new TextField(col[0], col[1].replace(":", "_"),Store.YES));
				}
			}
			
			documenst.add(doc);
			if(documenst.size()>5000){//使用批处理提交
				iw.addDocuments(documenst);
				documenst.clear();
			}
//	        context.write(null, null);
		 }
		
		@Override
        protected void cleanup(Context context)throws IOException, InterruptedException {
//            iw.commit();
			if(documenst.size()>0){
				iw.addDocuments(documenst);
			}
            if(iw!=null){
            	iw.close();//关闭至合并完成
            }
            
        }
	}
	
	/**
     * 获取一个IndexWriter
     * @param outDir 索引的输出目录
     * @return IndexWriter 获取一个IndexWriter
     * */
    public static IndexWriter getIndexWriter(String outDir) throws Exception{
        Analyzer  analyzer=new StandardAnalyzer();//IK分词
        IndexWriterConfig config=new IndexWriterConfig(analyzer);
        Configuration conf=new Configuration();
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
//        conf.set("fs.defaultFS","hdfs://192.168.46.32:9000/");//HDFS目录
        Path path=new Path("hdfs://hbzx/user/root/mrindex/index_"+outDir);//索引目录
        HdfsDirectory directory=new HdfsDirectory(path, conf);
        long heapSize = Runtime.getRuntime().totalMemory()/ 1024L / 1024L;//总内存
        long heapMaxSize = Runtime.getRuntime().maxMemory()/ 1024L / 1024L;//使用的最大内存
        config.setRAMBufferSizeMB(((heapMaxSize-heapSize)*0.7));//空闲内存的70%作为合并因子
        IndexWriter writer=new IndexWriter(directory, config);//
        return writer;
    }
    
    private static List<String> loadIndexField() {
		// TODO Auto-generated method stub
		List<String> fields = new ArrayList<String>();
		fields.add("AF_ESP");fields.add("AF_EXAC");fields.add("AF_TGP");fields.add("ALLELEID");
		fields.add("CLNDN");fields.add("CLNDNINCL");fields.add("CLNDISDB");fields.add("CLNDISDBINCL");fields.add("CLNHGVS");
		fields.add("CLNREVSTAT");fields.add("CLNSIG");fields.add("CLNSIGCONF");fields.add("CLNSIGINCL");fields.add("CLNVC");
		fields.add("CLNVCSO");fields.add("CLNVI");fields.add("DBVARID");fields.add("GENEINFO");fields.add("MC");
		fields.add("ORIGIN");fields.add("RS");fields.add("SSR");
		
//		fields.add("CIEND");fields.add("CIPOS");fields.add("END");
//		fields.add("IMPRECISE");fields.add("SVLEN");fields.add("SVTYPE");
//		fields.add("AC");fields.add("AF");fields.add("NS");
//		fields.add("AN");
		return fields;
	}
}
