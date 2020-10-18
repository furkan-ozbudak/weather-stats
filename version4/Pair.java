public class Pair {
	private float temperature;
	private String stationId;

	public Pair() {
	}

	public Pair(float temperature, String stationId) {
		super();
		this.temperature = temperature;
		this.stationId = stationId;
	}

	public float getTemperature() {
		return temperature;
	}

	public void setTemperature(float temperature) {
		this.temperature = temperature;
	}

	public String getStationId() {
		return stationId;
	}

	public void setStationId(String stationId) {
		this.stationId = stationId;
	}
}
