package AverageTemperature6;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable<CustomKey>{
	private Text stationID;
	private DoubleWritable temperature;
	
	public CustomKey() {
		stationID = new Text();
		temperature = new DoubleWritable();
	}
	
	public CustomKey(Text stationID, DoubleWritable temperature) {
		super();
		this.stationID = stationID;
		this.temperature = temperature;
	}
	
	public Text getStationID() {
		return stationID;
	}

	public void setStationID(Text stationID) {
		this.stationID = stationID;
	}

	public DoubleWritable getTemperature() {
		return temperature;
	}

	public void setTemperature(DoubleWritable temperature) {
		this.temperature = temperature;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		stationID.readFields(in);
		temperature.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		stationID.write(out);
		temperature.write(out);
		
	}

	@Override
	public int compareTo(CustomKey o) {
		if(this.stationID.equals(o.getStationID())) {
			return -(this.temperature.compareTo(o.getTemperature()));
		}
		else {
			return this.stationID.compareTo(o.getStationID());
		}
	}
	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((stationID == null) ? 0 : stationID.hashCode());
		result = prime * result
				+ ((temperature == null) ? 0 : temperature.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CustomKey other = (CustomKey) obj;
		if (stationID == null) {
			if (other.stationID != null)
				return false;
		} else if (!stationID.equals(other.stationID))
			return false;
		if (temperature == null) {
			if (other.temperature != null)
				return false;
		} else if (!temperature.equals(other.temperature))
			return false;
		return true;
	}

	@Override
	public String toString() {
		//String s = new DecimalFormat("#").format(stationID.get());
		return stationID.toString() + "      " + temperature;
	}


}
