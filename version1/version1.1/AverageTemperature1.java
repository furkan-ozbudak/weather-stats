package AverageTemperature1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageTemperature1 extends Configured implements Tool {

	public static class AverageTemperatureMapper extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		private final static DoubleWritable temperature = new DoubleWritable();
		private final static Text year = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			year.set(value.toString().substring(15, 19));
			temperature.set((Double.parseDouble(value.toString().substring(87,92))) / 10.0);
			context.write(year, temperature);
		}
	}

	public static class AverageTemperatureReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private final static DoubleWritable averageTemperature = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			double count = 0.0;
			for (DoubleWritable temperature : values) {
				sum += temperature.get();
				count++;
			}
			averageTemperature.set(sum / count);
			context.write(key, averageTemperature);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new AverageTemperature1(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[2]), true);

		Job job = new Job(getConf(), "AverageTemperature");
		job.setJarByClass(AverageTemperature1.class);

		job.setMapperClass(AverageTemperatureMapper.class);
		job.setReducerClass(AverageTemperatureReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
