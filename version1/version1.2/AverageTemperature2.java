package AverageTemperature2;

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

public class AverageTemperature2 extends Configured implements Tool {

	public static class AverageTemperatureMapper extends
			Mapper<LongWritable, Text, Text, Pair> {

		private final static IntWritable one = new IntWritable(1);
		private final static DoubleWritable temperature = new DoubleWritable();
		private final static Pair pair = new Pair();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Text year = new Text(value.toString().substring(15, 19));
			temperature.set((Double.parseDouble(value.toString().substring(87,
					92))) / 10.0);
			pair.setSum(temperature);
			pair.setCount(one);
			context.write(year, pair);
		}
	}

	public static class AverageTemperatureCombiner extends
			Reducer<Text, Pair, Text, Pair> {
		public void reduce(Text key, Iterable<Pair> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			int count = 0;
			for (Pair p : values) {
				sum += p.getSum().get();
				count += p.getCount().get();
			}
			context.write(key, new Pair(new DoubleWritable(sum), new IntWritable(count)));
		}
	}

	public static class AverageTemperatureReducer extends
			Reducer<Text, Pair, Text, DoubleWritable> {
		private final static DoubleWritable averageTemperature = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<Pair> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			for (Pair pair : values) {
				sum += pair.getSum().get();
				count += pair.getCount().get();
			}
			averageTemperature.set(sum / count);
			context.write(key, averageTemperature);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new AverageTemperature2(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[2]), true);

		Job job = new Job(getConf(), "AverageTemperature");
		job.setJarByClass(AverageTemperature2.class);

		job.setCombinerClass(AverageTemperatureCombiner.class);

		job.setMapperClass(AverageTemperatureMapper.class);
		job.setReducerClass(AverageTemperatureReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Pair.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
