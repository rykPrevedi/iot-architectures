package unimore.iot.architectures.tirocinio.hono.model;

/**
 * Basic and demo structure of a common message carrying a numeric value
 *
 * @author Riccardo Prevedi
 * @created 27/02/2023 - 14:27
 * @project architectures-iot
 */

public class MessageDescriptor {

    public static final String ENGINE_TEMPERATURE_SENSOR = "engine_temperature_sensor";

    public static final String HTTP_TEMPERATURE_SENSOR = "http_temperature_sensor";

    private long timestamp;

    private String type;

    private double value;

    public MessageDescriptor() {
    }

    public MessageDescriptor(String type, double value) {
        this.timestamp = System.currentTimeMillis();
        this.type = type;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("MessageDescriptor{");
        sb.append("timestamp=").append(timestamp);
        sb.append(", type='").append(type).append('\'');
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }
}
