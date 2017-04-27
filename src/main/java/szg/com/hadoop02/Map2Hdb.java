package szg.com.hadoop02;

import java.io.IOException;
import java.net.Socket;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;



public class Map2Hdb {
	public static class readfromHDBMapper extends TableMapper<ImmutableBytesWritable, Put> {
		private static final byte[] FAMILY = Bytes.toBytes("data");
		private static final byte[] QUALIFIER = Bytes.toBytes("content_data");
		private static final byte[] QUALIFIER1 = Bytes.toBytes("halfaddone");
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
				throws IOException, InterruptedException {
			String order_str = Bytes.toString((value.getValue(FAMILY, QUALIFIER)));
			int order = Integer.valueOf(order_str);
			int half = order / 2 +1;
			String half_str = Integer.toString(half);
			byte[] vl = Bytes.toBytes(half_str);
			ImmutableBytesWritable itbw = new ImmutableBytesWritable(value.getRow());
			Put put = new Put(value.getRow());
			put.addColumn(FAMILY, QUALIFIER1, vl);
			context.write(itbw, put);
		}
		private Socket socket;
		@Override
		protected void setup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
				throws IOException, InterruptedException {
			socket = new Socket("192.168.1.19", 8899);
		}
		
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "SimpleComputer");
		job.setJarByClass(Map2Hdb.class);
		Scan scan = new Scan();
		job.setNumReduceTasks(0);
		TableMapReduceUtil.initTableMapperJob("file_content", scan, readfromHDBMapper.class,
				ImmutableBytesWritable.class, Put.class, job);
		TableMapReduceUtil.initTableReducerJob("file_content", null, job);
		TableMapReduceUtil.addDependencyJars(job);
		System.out.println(job.waitForCompletion(true) ? 0 : 1);
	}
}
