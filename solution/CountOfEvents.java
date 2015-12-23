import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountOfEvents {

	public static class CountOfEventsMapper extends Mapper<Object, Text, Text, IntWritable> {
		private IntWritable one = new IntWritable(1);

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] item = value.toString().split(",");

			if (item.length != 4) {
				// in case of invalid input, increase robustness
				return;
			}

			context.write(new Text(item[1] + ',' + item[3]), one);						// key = [advertiserID],[type]  
		}
	}

	public static class CountOfEventsReducer extends Reducer<Text,IntWritable,NullWritable,Text> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count++;
			}
			String newKey = key.toString() + ',' + count;
			context.write(NullWritable.get(), new Text(newKey));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "count of events");
		job.setJarByClass(CountOfEvents.class);
		job.setMapperClass(CountOfEventsMapper.class);
		job.setReducerClass(CountOfEventsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
