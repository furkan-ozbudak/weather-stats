package AverageTemperature4;

import java.io.IOException;
import java.util.HashMap;
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

public class AverageTemperature4 extends Configured implements Tool {

	public static class AverageTemperatureMapper extends
			Mapper<LongWritable, Text, Key, Pair> {
		private HashMap<Text, Pair> map;
		private final static IntWritable one = new IntWritable(1);

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			map = new HashMap<>();
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Text year = new Text(value.toString().substring(15, 19));
			double t = ((Double.parseDouble(value.toString().substring(87, 92))) / 10.0);

			if (map.containsKey(year)) {
				Pair p = map.get(year);
				map.put(year, new Pair(
						new DoubleWritable(p.getSum().get() + t),
						new IntWritable(p.getCount().get() + 1)));
			} else {
				map.put(year, new Pair(new DoubleWritable(t), one));
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Map.Entry<Text, Pair> e : map.entrySet()) {
				context.write(new Key(new IntWritable(Integer.parseInt(e.getKey().toString()))), e.getValue());
			}
		}
	}

	public static class AverageTemperatureReducer extends
			Reducer<Key, Pair, Text, DoubleWritable> {
		private final static DoubleWritable averageTemperature = new DoubleWritable();

		@Override
		public void reduce(Key key, Iterable<Pair> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			for (Pair pair : values) {
				sum += pair.getSum().get();
				count += pair.getCount().get();
			}
			averageTemperature.set(sum / count);
			context.write(new Text(key.toString()), averageTemperature);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new AverageTemperature4(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[2]), true);

		Job job = new Job(getConf(), "AverageTemperature");
		job.setJarByClass(AverageTemperature4.class);

		job.setMapperClass(AverageTemperatureMapper.class);
		job.setReducerClass(AverageTemperatureReducer.class);

		job.setMapOutputKeyClass(Key.class);
		job.setMapOutputValueClass(Pair.class);

		job.setOutputKeyClass(Key.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
