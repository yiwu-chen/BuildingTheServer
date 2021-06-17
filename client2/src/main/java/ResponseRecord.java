/**
 * The class that represents the response result which includes start time, latency,
 * response type, and response code.
 */
public class ResponseRecord {
  private long startTime;
  private long latency;
  private String type;
  private int responseCode;

  public ResponseRecord(long startTime, String type, long latency, int ResponseCode) {
    this.startTime = startTime;
    this.type = type;
    this.latency = latency;
    this.responseCode = ResponseCode;
  }

  public long getLatency() {
    return latency;
  }

  public long getStartTime() {
    return startTime;
  }

  @Override
  public String toString() {
    return startTime + "\t," + type + "\t," + latency + "\t," + responseCode + "\n";
  }
}
