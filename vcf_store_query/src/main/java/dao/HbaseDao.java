package dao;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import model.Row;

public class HbaseDao {
	
	public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    
    public static Connection getConnection() {
		return connection;
	}

	public static void setConnection(Connection connection) {
		HbaseDao.connection = connection;
	}

	public static void main(String[] args) throws Exception {
        init();
        HbaseDao dao = new HbaseDao();
//        //列出所有表
//        dao.listTables();
        //创建testTable表
        dao.createTable("test_mp", "data","info");
//        //列出所有表
//        dao.listTables();
//        
//        //插入表数据
//        dao.insertRow("testTable", "row1", "cf1", "col11", "valuecol11");
//        dao.insertRow("testTable", "row1", "cf1", "col12", "valuecol12");
//        dao.insertRow("testTable", "row1", "cf2", "", "valuecf2");
//        dao.insertRow("testTable", "row2", "cf1", "", "valueerow2");
//        //删除表数据
//        dao.deleteRow("testTable", "row1", "cf1", "col11");
//        dao.deleteRow("testTable", "row2", null, null);
//        dao.deleteRow("testTable", "row1", "cf2",null);
//        //获取某行数据
//        dao.getData("testTable", "row1");
//        dao.getData("testTable", "row1", "cf1");
//        
//        //删除testTable
//        dao.deleteTable("testTable");
//        //列出所有表
//        dao.listTables();
//        
        close();
    }
    
    //建立连接
	
    //建立连接
    public static void init(){
        configuration  = HBaseConfiguration.create();
        try{
        	//伪分布测试环境
//            configuration.set("hbase.rootdir","hdfs://mad:9000/hbase");
            //分布式测试环境
//            configuration.set("fs.defaultFS", "hdfs://node101:9006/");
//            configuration.set("hbase.zookeeper.quorum", "node2,node3,node4");
//            configuration.set("hbase.rootdir","hdfs://node101:9006/hbase");
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    
    //关闭连接
    public static void close(){
        try{
            if(admin != null){
                admin.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    
    /**
     * 建表。HBase的表中会有一个系统默认的属性作为主键，主键无需自行创建，默认为put命令操作中表名后第一个数据，因此此处无需创建id列
     * @param myTableName 表名
     * @param colFamily 列族名
     * @throws IOException
     */
    public void createTable(String myTableName,String... cfs) throws IOException {
 
//        init();
        TableName tableName = TableName.valueOf(myTableName);
 
        if(admin.tableExists(tableName)){
            System.out.println("talbe is exists!");
        }else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for(String cf:cfs){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
            System.out.println("create table success");
        }
//        close();
    }
    
    /**
     * 删除指定表
     * @param tableName 表名
     * @throws IOException
     */
    public void deleteTable(String tableName) throws IOException {
//        init();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
            System.out.println("Delete table "+tableName+" successfully.");
        }
//        close();
    }
 
    /**
     * 查看已有表
     * @throws IOException
     */
    public void listTables() throws IOException {
        init();
        HTableDescriptor hTableDescriptors[] = admin.listTables();
        for(HTableDescriptor hTableDescriptor :hTableDescriptors){
            System.out.println(hTableDescriptor.getNameAsString());
        }
        /*HTableDescriptor hTableDescriptors = admin.getTableDescriptor(TableName.valueOf("Student"));
        HColumnDescriptor[] hColumnDescriptor = hTableDescriptors.getColumnFamilies();
        for (HColumnDescriptor cName : hColumnDescriptor) {   
        	String name = Bytes.toString(cName.getName());
        	System.out.println(name);
        	Map<ImmutableBytesWritable,ImmutableBytesWritable> m = cName.;
        	for (ImmutableBytesWritable key : m.values()) {
        		System.out.println(Bytes.toString(key.get()));
        	}
        }*/
        
//        close();
    }

    /**
     * 向某一行的某一列插入数据
     * @param tableName 表名
     * @param rowKey 行键
     * @param colFamily 列族名
     * @param col 列名（如果其列族下没有子列，此参数可为空）
     * @param val 值
     * @throws IOException
     */
    public void insertRow(String tableName,String rowKey,String colFamily,String col,String val) throws IOException {
//        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
//        close();
    }
 
    /**
     * 删除数据
     * @param tableName 表名
     * @param rowKey 行键
     * @param colFamily 列族名[可选]
     * @param col 列名[可选，但是与colFamily一起出现]
     * @throws IOException
     */
    public void deleteRow(String tableName,String rowKey,String colFamily,String col) throws IOException {
//        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        if(null!=colFamily && null == col){
        	//删除指定列族的所有数据
            delete.addFamily(colFamily.getBytes());
        }else if(null!=colFamily && null!= col){
        	//删除指定列的数据
            delete.addColumn(colFamily.getBytes(), col.getBytes());
        }
        
        table.delete(delete);
        table.close();
//        close();
    }
    
    /**
     * 根据行键rowkey查找数据
     * @param tableName 表名
     * @param rowKey 行键
     * @param colFamily 列族名[可选 多个 选择显示列族]
     * @throws IOException
     */
    public Row getData(String tableName,String rowKey,String... cfs)throws  IOException{
//        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        for(String cf : cfs){
        	get.addFamily(cf.getBytes());
        }
        Result result = table.get(get);
        //showCell(result);
        Map<byte[],Map<byte[], byte[]>> values = new HashMap<byte[],Map<byte[], byte[]>>();
        //获取列族信息
        HTableDescriptor hTableDescriptor=table.getTableDescriptor();
    	for(HColumnDescriptor fdescriptor : hTableDescriptor.getColumnFamilies()){
    		byte[] cf = fdescriptor.getName();
    		Map<byte[], byte[]> cols = result.getFamilyMap(cf);
    		values.put(cf, cols);
    	}
        table.close();
        return new Row(result.getRow(),values);
    }
    
    /**
     * 根据行键rowkey查找数据显示某列族某列数据
     * @param tableName 表名
     * @param rowKey 行键
     * @param colFamily 列族名
     * @param col 列名
     * @throws IOException
     */
    public void getData(String tableName,String rowKey,String colFamily,String col)throws  IOException{
//        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(colFamily.getBytes(),col.getBytes());
        Result result = table.get(get);
        showCell(result);
        table.close();
//        close();
    }
    
    /**
     * 格式化输出
     * @param result
     */
    public void showCell(Result result){
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
            System.out.println("Timetamp:"+cell.getTimestamp()+" ");
            System.out.println("column Familyj:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
        }
    }
    
    /**
     * 根据Result获得数据
     * @param result
     * @return
     */
    public String showData(Result result){
    	String resultText = "";
        Cell[] cells = result.rawCells();
        for(Cell cell : cells) {
        	resultText = resultText + Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "	";
        }
        
        resultText += "\n";
        for(Cell cell : cells) {
        	resultText = resultText + Bytes.toString(CellUtil.cloneValue(cell)) + "	";
        }
        //close();
        
    	return resultText; 
    }
    
    /**
     * 根据行键获得数据
     * @param tablename
     * @param rowkey
     * @return
     * @throws IOException
     */
    public String getDataByRowkey(String tablename, String rowkey) throws IOException {
    	
    	String resultText = "";
    	//init();
    	Table table = connection.getTable(TableName.valueOf(tablename));
        Get get = new Get(rowkey.getBytes());
        Result r = table.get(get);
        Cell[] cells = r.rawCells();
        for(Cell cell : cells) {
        	resultText = resultText + Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "	";
        }
        
        resultText += "\n";
        for(Cell cell : cells) {
        	resultText = resultText + Bytes.toString(CellUtil.cloneValue(cell)) + "	";
        }
        
        //close();
    	return resultText;
    }
    
    /**
     * 包含运算符的rowkey的模糊查询,仅针对数值型的查询
     * @param tablename
     * @param filterValue
     * @param operator
     * @return
     * @throws IOException
     */
    public HashSet<String> filterScan(String tablename, String key, String value, String operator) throws IOException {
    	HashSet<String> result = new HashSet<String>();
    	String filterValue = "";
    	//init();
    	Table table = connection.getTable(TableName.valueOf(tablename));
    	Scan scan = new Scan();
    	Filter filter = null;
    	//注意是字节的比较，因此模糊查询的<=和>要稍作修改
    	if(operator.equals("<")) {
    		filterValue = key + "," + convert(value);
    		filter = new RowFilter(CompareOp.LESS, new BinaryComparator(filterValue.getBytes())); 
    	}
    	if(operator.equals("<=")) {
    		filterValue = key + "," + convert(Integer.parseInt(value) + 1 + "");
    		filter = new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(filterValue.getBytes())); 
    	}
    	if(operator.equals(">")) {
    		filterValue = key + "," + convert(Integer.parseInt(value) + 1 + "");
    		filter = new RowFilter(CompareOp.GREATER, new BinaryComparator(filterValue.getBytes())); 
    	}
    	if(operator.equals(">=")) {
    		filterValue = key + "," + convert(value);
    		filter = new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(filterValue.getBytes())); 
    	}
    	//当为等于时，这里应该为包含，而不应该为等于，因为是模糊查询
    	if(operator.equals("=")) {
    		filterValue = key + "," + convert(value);
    		filter = new RowFilter(CompareOp.EQUAL, new SubstringComparator(filterValue)); 
    	}
    	scan.setFilter(filter);
    	ResultScanner rs = table.getScanner(scan);
    	for(Result r : rs) {
    		//result.add(r);
    		result.add(Bytes.toString(r.getRow()));
    	}
    	//close();
    	
    	return result;
    }
    
    private String convert(String val) {
		int len = val.toCharArray().length;
		for(int i=0;i<32-len;i++) {
			val = "0" + val;
		}
		return val;
	}
    

}
