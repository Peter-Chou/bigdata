package peterchou.flink.stream.beams.sensor;

public class SensorData {
  private String id;
  private Long timeStamp;
  private Double temperature;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Long getTimeStamp() {
    return timeStamp;
  }

  public void setTimeStamp(Long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public Double getTemperature() {
    return temperature;
  }

  public void setTemperature(Double temperature) {
    this.temperature = temperature;
  }

  public SensorData(String id, Long timeStamp, Double temperature) {
    this.id = id;
    this.timeStamp = timeStamp;
    this.temperature = temperature;
  }

  @Override
  public String toString() {
    return "SensorData{timeStamp=" + timeStamp + ", " + "id=" + id + ", " + "temperature=" + temperature + "}";
  }

}
