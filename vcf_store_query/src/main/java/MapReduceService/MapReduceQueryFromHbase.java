package MapReduceService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * 利用MapReduce查询hbase
 * @author ssh
 *
 */
public class MapReduceQueryFromHbase {
	
	private static ArrayList<Character> symbolSet = new ArrayList<Character>();  //保存查询语句中语句与语句间的逻辑关系符号
	//以下3个集合是一一对应的
	private static ArrayList<String> keySet = new ArrayList<String>();        //保存查询语句中的关系运算符左侧的字符串，对应于hbase中表的列限定符名称
	private static ArrayList<String> opSet = new ArrayList<String>();          //保存查询语句中的关系运算符
	private static ArrayList<String> valueSet = new ArrayList<String>();    //保存查询语句中的关系运算符右侧的值
	//private String family = null;
	private static ArrayList<String> familySet = new ArrayList<String>();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long t1 = new Date().getTime();
		new MapReduceQueryFromHbase().vcfQuery(args[0],args[1]);
		long t2 = new Date().getTime();
		System.out.println("QueryByMapred cost time "+(t2-t1)/1000+"s");
	}
	
	public void vcfQuery(String queryStatement,String tablename) {
		long startTime = System.currentTimeMillis();
	
		Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node2.hadoop,node3.hadoop,node4.hadoop");
        conf.set("hbase.rootdir","hdfs://node1.hadoop:9000/hbase");
        
        //将需要向集群中传递的参数通过configuration的set函数传入，并在需要的地方通过get函数获得
        conf.set("queryStatement", queryStatement);
		
		try {
		    String[] IOArgs = new String[] {"input", "output"};
		    String[] otherArgs = new GenericOptionsParser(conf,IOArgs).getRemainingArgs();
			if(otherArgs.length != 2) {
				System.err.println("Usage: QueryWithMR <query> <tablename>"); 
				System.exit(2);
			}
			
			Job job = Job.getInstance(conf,"QueryWithMR");
			job.setJarByClass(MapReduceQueryFromHbase.class);
			Scan sc = new Scan();
			
			TableMapReduceUtil.initTableMapperJob(tablename, sc, QueryMapper.class, Text.class, Text.class, job);
			//job.setReducerClass(QueryReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileSystem fs=FileSystem.get(conf);
			Path p=new Path(otherArgs[1]);
	        if(fs.exists(p)){
	            fs.delete(p, true);
	            System.out.println("输出路径存在，已删除！");
	        }
			FileOutputFormat.setOutputPath(job, p);
			
			if(job.waitForCompletion(true)) {
				long endTime = System.currentTimeMillis();
			    System.out.println("mapRedQueryFromHbase程序运行的时间是：" +  (endTime - startTime) + "ms");
			    System.exit(0);
			}else {
				System.exit(1);
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		 } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
	    }catch (IOException e) {
		// TODO Auto-generated catch block
	    	e.printStackTrace();
	 }
	}

	public static class QueryMapper extends TableMapper<Text,Text> {
		
		String queryStatement = null;
		String[] COLFAMILIES = {"data","info","sample"};
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//获得vcf的查询语句，该语句可能为多个语句的“与”或“或”的组合，如 vcf:POS>17330&vcf:POS<1230237等
			String queryStatement = conf.get("queryStatement");
			
			for(int i=0;i<queryStatement.length();i++) {
				if(queryStatement.charAt(i) == '&' || queryStatement.charAt(i) == '|') {
					symbolSet.add(queryStatement.charAt(i));
				}
			}
			
			String[] splitStatement = queryStatement.split("[&|]");
			for(String s : splitStatement) {
				System.out.println(s);
			}
			for(String e : splitStatement) {
				String op = "";
				//提取语句中的比较符号
				for(int j=0;j<e.length();j++) {                                                         
					if(e.charAt(j) == '>' || e.charAt(j) == '<' || e.charAt(j) == '=' || e.charAt(j) == '!') {
					   op = op + e.charAt(j); 
					}
				}
				opSet.add(op);
				String[] splitS = e.split("[><=!]");
				for(String ss : splitS) {
					System.out.println(ss);
				}
				//此处需要注意！带"<=",">="的语句如vcf:QUAL<=1000在拆分后形成的数组元素个数为3,并不是2.分别为"vcf:QUAL","","1000"
				//此时的值是数组第三个元素，若按第二个元素则值为空
				if(op.length() == 2) {
					valueSet.add(splitS[2]);
				}else {
					valueSet.add(splitS[1]);
				}
				//valueSet.add(splitS[1]);
				String[] familycol = splitS[0].split(":");
				familySet.add(familycol[0]);
				//family = familycol[0];
				keySet.add(familycol[1]);
				
			}
		}
		
		@Override
		public void map(ImmutableBytesWritable key, Result rs, Context context) throws IOException, InterruptedException {
			boolean flag = isMatch(0, familySet.get(0), rs);
			//根据语句中的逻辑运算符计算flag值
			for(int i=0;i<symbolSet.size();i++) {
				boolean f = isMatch(i+1, familySet.get(i+1), rs);
				if(symbolSet.get(i) == '&') {
					flag = flag && f;
				}
				if(symbolSet.get(i) == '|') {
					flag = flag || f;
				}
			}
			//若flag为true，证明该行满足条件，输出该行数据
			if(flag) {
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
				
				context.write(new Text(rowinfo.substring(0, rowinfo.length()-1)+"}"), new Text(""));
			}
		}

		//判断查询语句是否符合表中该行内容
		private boolean isMatch(int i, String family,Result values) {
			if(queryStatement == null) {
				return false;
			}
			boolean flag = true;
			String value = Bytes.toString(values.getValue(family.getBytes(), keySet.get(i).getBytes()));
			if(value.equals("") || value.contains(",")|| valueSet.get(i).equals("") || valueSet.isEmpty()) {
				return false;
			}
			if(opSet.get(i).equals(">")) {
				flag = (Double.parseDouble(value) > Double.parseDouble(valueSet.get(i)));
			}
			if(opSet.get(i).equals(">=")) {
				flag = (Double.parseDouble(value) >= Double.parseDouble(valueSet.get(i)));
			}
			if(opSet.get(i).equals("<")) {
				flag = (Double.parseDouble(value) < Double.parseDouble(valueSet.get(i)));
			}
			if(opSet.get(i).equals("<=")) {
				flag = (Double.parseDouble(value) <= Double.parseDouble(valueSet.get(i)));
			}
			if(opSet.get(i).equals("=")) {
				flag = value.equals(valueSet.get(i));
			}
			return flag;
		}
		
	}
}
