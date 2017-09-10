package liuxun.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HBaseDemo {
	private Configuration conf = null;
	@Before
	public void init() {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop4:2181,hadoop5:2181,hadoop6:2181");
	}
	/**
	 * 插入单条数据
	 * @throws Exception 
	 */
	@Test
	public void testPut() throws Exception {
		HTable table = new HTable(conf, "peoples");
		Put put = new Put(Bytes.toBytes("kr0001")); //指定row key
		put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("张三"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(35));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes(150000.0));
		table.put(put);
		table.close();
	}
	/**
	 * 插入多条数据
	 * @throws Exception
	 */
	@Test
	public void testPutAll() throws Exception {
		HTable table = new HTable(conf, "peoples");
//		List<Put> puts = new ArrayList<>();
//		for(int i = 1; i <= 1000000; i++) {
//			Put put = new Put(Bytes.toBytes("kr"+i));
//			put.add(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes(""+i));
//			puts.add(put);
//		}
//		table.put(puts);
//		table.close();
		List<Put> puts = new ArrayList<>(10000);
		for (int i = 1; i <= 1000000; i++) {
			Put put = new Put(Bytes.toBytes("kr" + i));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes("" + i));
			puts.add(put);
			if (i % 10000 == 0) {
				table.put(puts);
				puts = new ArrayList<>(10000);
			}
		}
		table.put(puts);
		table.close();
	}
	
	/**
	 * 根据row key查询单条数据
	 * @throws Exception
	 */
	@Test
	public void testGet() throws Exception {
		HTable table = new HTable(conf, "peoples");
		Get get = new Get(Bytes.toBytes("kr1000000")); //根据主键查询
		Result result = table.get(get);
		String money = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("money"))); 
		System.out.println(money);
		table.close();
	}
	
	/**
	 * 查询多条数据
	 * @throws Exception
	 */
	@Test
	public void testScan() throws Exception {
		HTable table = new HTable(conf, "peoples");
		Scan scan = new Scan(Bytes.toBytes("kr299990"), Bytes.toBytes("kr300000"));
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			String money = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("money"))); 
			System.out.println(money);
		}
		table.close();
	}
	
	/**
	 * 删除数据 
	 * 注意：并没有真正删除，而只是做了一个标志位，只有flush清空内存的时候才会真正删除
	 * @throws Exception
	 */
	@Test
	public void testDel() throws Exception {
		HTable table = new HTable(conf, "peoples");
		Delete delete = new Delete(Bytes.toBytes("kr1000000"));
		table.delete(delete);
		table.close();
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		// 通过ZooKeeper确定连接地址，配置内容从hbase-site.xml中查找
		conf.set("hbase.zookeeper.quorum", "hadoop4:2181,hadoop5:2181,hadoop6:2181");
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("peoples"));
		HColumnDescriptor family_info = new HColumnDescriptor("info");
		family_info.setMaxVersions(3);
		HColumnDescriptor family_data = new HColumnDescriptor("data");
		desc.addFamily(family_info);
		desc.addFamily(family_data);
		admin.createTable(desc);
		admin.close();
	}
}
