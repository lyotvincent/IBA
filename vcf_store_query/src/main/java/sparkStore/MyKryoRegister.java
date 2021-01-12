package sparkStore;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;

public class MyKryoRegister implements KryoRegistrator {

	public void registerClasses(Kryo ky) {
		// TODO Auto-generated method stub
		ky.register(org.apache.lucene.document.Document.class);
		ky.register(org.apache.hadoop.hbase.io.ImmutableBytesWritable.class);
	}

}
