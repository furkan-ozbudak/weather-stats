package AverageTemperature4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Key implements WritableComparable<Key>{
	private IntWritable year;
	
	public Key() {
		year = new IntWritable();
	}

	public Key(IntWritable year) {
		super();
		this.year = year;
	}

	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = year;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		year.readFields(in);	
	}

	@Override
	public void write(DataOutput out) throws IOException {
		year.write(out);	
	}
	
	@Override
	public String toString() {
		return year.toString();
	}

	@Override
	public int compareTo(Key o) {
		return -(this.year.compareTo(o.getYear()));
	}

}
