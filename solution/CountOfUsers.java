import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountOfUsers {

	private static class CountOfUsersKey implements Writable, WritableComparable<CountOfUsersKey> {
		private Text adAndType;
		private Text userID;

		public CountOfUsersKey() {
			this.adAndType = new Text();
			this.userID = new Text();
		}

		public CountOfUsersKey(String adAndType, String userID) {
			this.adAndType = new Text(adAndType);
			this.userID = new Text(userID);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			adAndType.write(out);
			userID.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			adAndType.readFields(in);
			userID.readFields(in);
		}

		@Override
		public int compareTo(CountOfUsersKey o) {
			int compareAdAndType = this.adAndType.compareTo(o.adAndType);
			if (compareAdAndType != 0) {
				return compareAdAndType;
			}
			return this.userID.compareTo(o.userID);
		}

		@Override
		public int hashCode() {
			return (adAndType.toString() + userID.toString()).hashCode();
		}

		public Text getAdAndType() {
			return adAndType;
		}
	}

	public static class CountOfUsersMapper extends Mapper<Object, Text, CountOfUsersKey, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] item = value.toString().split(",");
			// key = [advertiserID],[type] + [userID]	value = [userID]
			context.write(new CountOfUsersKey(item[1] + ',' + item[3], item[2]), new Text(item[2]));		
		}
	}

	public static class CountOfUsersPartitioner extends Partitioner<CountOfUsersKey, Text> {

		@Override
		public int getPartition(CountOfUsersKey key, Text value, int numReduceTasks) {
			return key.getAdAndType().hashCode();
		}
	}

	public static class CountOfUsersGroupComparator extends WritableComparator {
		
		protected CountOfUsersGroupComparator() {  
			super(CountOfUsersKey.class, true);  
		}

		@Override
		public int compare(WritableComparable key1, WritableComparable key2) {
			return (((CountOfUsersKey)key1).getAdAndType()).compareTo(((CountOfUsersKey)key2).getAdAndType());
		}
	}

	public static class CountOfUsersReducer extends Reducer<CountOfUsersKey,Text,NullWritable,Text> {
		@Override
		public void reduce(CountOfUsersKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String prev = "";
			int count = 0;
			for (Text val : values) {
				String valString = val.toString();
				if (!prev.equals(valString)) {
					count++;
					prev = valString;
				}
			}
			context.write(NullWritable.get(), new Text(key.getAdAndType().toString() + ',' + count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "count of unique users");
		job.setJarByClass(CountOfUsers.class);
		job.setMapperClass(CountOfUsersMapper.class);
		job.setPartitionerClass(CountOfUsersPartitioner.class);
		job.setGroupingComparatorClass(CountOfUsersGroupComparator.class);
		job.setReducerClass(CountOfUsersReducer.class);
		job.setOutputKeyClass(CountOfUsersKey.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
