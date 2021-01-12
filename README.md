# IBA
1. SqldbTest工程测试的是Mysql和Oracle存储和查询VCF文件的程序  
	--dao  
		--BaseDao：连接数据库Mysql的dao  
		--OracleDao：连接数据库Oracle的dao  
	--MysqlTest  
		--Mysql1000g.java :store方法存储VCF文件；query方法查询VCF文件（这个没有测试，因为存储方面Spark和MapReduce有优势就没有测试啦）  
	--OracleTest  
		--Oracle1000g.java：store方法存储VCF文件；query方法查询VCF文件（这个没有测试，因为存储方面Spark和MapReduce有优势就没有测试啦）  
	
2. vcf工程测试Spark和MapReduce程序  
	--dao  
		--BaseDao：连接数据库Mysql的dao  
		--HbaseDao：连接数据库HBase的dao  
	--MapReduceService  
		--MapReduceStoreToHbase.java：MapReduce存储VCF到Hbase，没有测试  
		--MapReduceQueryFromHbase.java:MapReduce查询VCF  
	--model：使用到的DTO  
	--sematicExpansion  
		--SimNetwork.java：根据doid找到可语义拓展的OMIMID。没有编写一整套的语义拓展程序，得到拓展的OMIMID后手动拓展的，如有其它需要请进一步改进。> <  
	--sparkQuery  
		--QueryBySparkFromHBase.java：Spark查询VCF  
		--QueryBySparkFromHBaseByIndex：通过索引查询VCF  
	--sparkStore  
		--MyKryoRegister.java：Kryo序列化注册器  
		--ReadAndIndexHDFS.java：构建INFO列索引  
		--SparkStoreBulkload：Spark初次批量导入VCF到HBase，最终测试的程序  
		--StoreVCFBySpark.java：Spark通过Hbase API存储VCF，超级慢  
		
	--src/main/resources：集群配置文件  
