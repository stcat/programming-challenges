import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Preprocess {

	private static final String separator = ";";

	public static class PreprocessMapper extends Mapper<Object, Text, Text, Text> {
		private Text keyWord = new Text();
		private Text valueWord = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] item = value.toString().split(",");

			if (item.length < 4) {
				// in case of invalid input, increase robustness
				return;
			}

			// insert some leading zeros to the timestamp in order to compare
			StringBuilder tmp = new StringBuilder();
			for (int i = item[0].length(); i < 10; i++) { 								// ten digits timestamp, will be enough in 270 years
				tmp.append('0');
			}
			tmp.append(item[0]);
			String timestamp = tmp.toString();

			// distinguish impression and event based on the number of ','
			switch (item.length) {
				case 4:	
					// This is an impression										
					keyWord.set(item[1] + "," + item[3] + separator + timestamp);		// key = [advertiserID],[userID];[timestamp]
					valueWord.set(item[0] + ",impression");								// value = [timestamp],impression
					context.write(keyWord, valueWord);
					break;
				case 5:	
					// This is an event										
					keyWord.set(item[2] + "," + item[3] + separator + timestamp);		// key = [advertiserID],[userID];[timestamp]
					valueWord.set(item[0] + "," + item[4]);								// value = [timestamp],[eventType]
					context.write(keyWord, valueWord);
					break;
				default:																// should not execute 														
			};
		}
	}

	//Split the key into natural and augment
	private static String getNaturalKey(Text compositeKey) {

		String compositeString = compositeKey.toString();
		int partition = compositeString.indexOf(separator);

		if (partition == -1) { 															// should not execute
			return compositeString;
		}

		return compositeString.substring(0, partition);
	}

	public static class PreprocessPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text compositeKey, Text value, int numReduceTasks) {
			return getNaturalKey(compositeKey).hashCode() % numReduceTasks;
		}
	}

	public static class PreprocessGroupComparator extends WritableComparator {

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return 0;
		}
		@Override
		public int compare(WritableComparable compositeKey1, WritableComparable compositeKey2) {
			return getNaturalKey((Text)compositeKey1).compareTo(getNaturalKey((Text)compositeKey2));
		}
	}

	public static class PreprocessReducer extends Reducer<Text,Text,NullWritable,Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			boolean impressioned = false;
			Map<String, Long> prev = new HashMap<String, Long>();

			for (Text val : values) {
				String valString = val.toString();
				int partition = valString.indexOf(',');

				if (partition == -1) { 													// should not execute
					continue;
				}

				long timestamp = Long.parseLong(valString.substring(0, partition));
				String type = valString.substring(partition + 1);

				if (type.equals("impression")) {
					impressioned = true;
				} else if (impressioned) {
					if (!prev.containsKey(type) || timestamp > prev.get(type) + 60) {
						String[] keyString = key.toString().split(separator);
						StringBuilder tmp = new StringBuilder();
						tmp.append(keyString[1]);
						tmp.append(',');
						tmp.append(keyString[0]);
						tmp.append(',');
						tmp.append(type);
						context.write(NullWritable.get(), new Text(tmp.toString()));
					}
					prev.put(type, timestamp);
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
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
