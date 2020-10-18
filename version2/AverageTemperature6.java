package AverageTemperature6;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

public class AverageTemperature6 extends Configured implements Tool {

	public static class AverageTemperatureMapper extends
			Mapper<LongWritable, Text, CustomKey, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Text s = new Text(value.toString().substring(4, 15)); // stationID
			DoubleWritable t = new DoubleWritable((Double.parseDouble(value
					.toString().substring(87, 92))) / 10); // temperature
			Text y = new Text(value.toString().substring(15, 19)); // year
			context.write(new CustomKey(s, t), y);
		}
	}

	public static class AverageTemperatureReducer extends
			Reducer<CustomKey, Text, Text, NullWritable> {

		@Override
		public void reduce(CustomKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String sID1 = key.getStationID().toString().substring(0, 6);
			String sID2 = key.getStationID().toString().substring(6);
			String sID3 = sID1 + "-" + sID2;
			String output = "";
			for (Text year : values) {
				output += sID3 + "        " + key.getTemperature().toString()
						+ "     " + year.toString().toString() + "\n";
			}
			context.write(new Text(output), NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new AverageTemperature6(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[2]), true);

		Job job = new Job(getConf(), "AverageTemperature");
		job.setJarByClass(AverageTemperature6.class);

		job.setMapperClass(AverageTemperatureMapper.class);
		job.setReducerClass(AverageTemperatureReducer.class);

		job.setMapOutputKeyClass(CustomKey.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
