import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

public class Preprocess {

	private static class MyKey implements Writable, WritableComparable<MyKey> {
		private Text adAndUserID;
		private LongWritable timestamp;

		public MyKey() {
			this.adAndUserID = new Text();
			this.timestamp = new LongWritable();
		}

		public MyKey(String adAndUserID, String timestamp) {
			this.adAndUserID = new Text(adAndUserID);
			this.timestamp = new LongWritable(Long.parseLong(timestamp));
		}

		@Override
		public void write(DataOutput out) throws IOException {
			adAndUserID.write(out);
			timestamp.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			adAndUserID.readFields(in);
			timestamp.readFields(in);
		}

		@Override
		public int compareTo(MyKey o) {
			int compareAdandUser = this.adAndUserID.compareTo(o.adAndUserID);
			if (compareAdandUser != 0) {
				return compareAdandUser;
			}
			return this.timestamp.compareTo(o.timestamp);
		}

		@Override
		public int hashCode() {
			return (adAndUserID.toString() + timestamp.get()).hashCode();
		}

		public Text getAdAndUser() {
			return adAndUserID;
		}

		public LongWritable getTimestamp() {
			return timestamp;
		}
	}

	public static class PreprocessMapper extends Mapper<Object, Text, MyKey, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] item = value.toString().split(",");

			if (item.length < 4) {
				// in case of invalid input, increase robustness
				return;
			}

			// distinguish impression and event based on the number of ','
			switch (item.length) {
				case 4:	
					// This is an impression
					// key = [advertiserID],[userID] + [timestamp] 
					// value = impression
					context.write(new MyKey(item[1] + "," + item[3], item[0]), new Text("impression"));
					break;
				case 5:	
					// This is an event
					// key = [advertiserID],[userID] + [timestamp] 
					// value = [eventType]
					context.write(new MyKey(item[2] + "," + item[3], item[0]), new Text(item[4]));
					break;
				default:																// should not execute
			};
		}
	}

	public static class PreprocessPartitioner extends Partitioner<MyKey, Text> {

		@Override
		public int getPartition(MyKey key, Text value, int numReduceTasks) {
			return key.getAdAndUser().hashCode();
		}
	}

	public static class PreprocessGroupComparator extends WritableComparator {
		
		protected PreprocessGroupComparator() {  
			super(MyKey.class, true);  
		}

		@Override
		public int compare(WritableComparable key1, WritableComparable key2) {
			return (((MyKey)key1).getAdAndUser()).compareTo(((MyKey)key2).getAdAndUser());
		}
	}

	public static class PreprocessReducer extends Reducer<MyKey,Text,NullWritable,Text> {
		@Override
		public void reduce(MyKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			boolean impressioned = false;
			Map<String, Long> prev = new HashMap<String, Long>();

			for (Text val : values) {
				LongWritable timestamp = key.getTimestamp();
				String type = val.toString();

				if (type.equals("impression")) {
					impressioned = true;
				} else if (impressioned) {
					long time = timestamp.get();
					if (!prev.containsKey(type) || time > prev.get(type) + 60) {
						StringBuilder tmp = new StringBuilder();
						tmp.append(time);
						tmp.append(',');
						tmp.append(key.getAdAndUser().toString());
						tmp.append(',');
						tmp.append(type);
						context.write(NullWritable.get(), new Text(tmp.toString()));
					}
					prev.put(type, time);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "preprocess");
		job.setJarByClass(Preprocess.class);
		job.setMapperClass(PreprocessMapper.class);
		job.setPartitionerClass(PreprocessPartitioner.class);
		job.setGroupingComparatorClass(PreprocessGroupComparator.class);
		job.setReducerClass(PreprocessReducer.class);
		job.setOutputKeyClass(MyKey.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
