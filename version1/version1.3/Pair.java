package AverageTemperature3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class Pair implements Writable{
	private DoubleWritable sum;
	private IntWritable count;
	
	public Pair() {
		sum = new DoubleWritable(0.0);
		count = new IntWritable(0);
	}

	public Pair(DoubleWritable s, IntWritable c) {
		sum = s;
		count = c;
	}
	
	public DoubleWritable getSum() {
		return sum;
	}

	public void setSum(DoubleWritable sum) {
		this.sum = sum;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sum.readFields(in);
		count.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		sum.write(out);
		count.write(out);
	}

}
