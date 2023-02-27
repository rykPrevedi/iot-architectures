package unimore.iot.architectures.tirocinio.hono.devices.mqtt.model;

/**
 * Basic and demo structure of a common message carrying a numeric value
 *
 * @author Riccardo Prevedi
 * @created 27/02/2023 - 14:27
 * @project architectures-iot
 */

public class MessageDescriptor {

    public static final String ENGINE_TEMPERATURE_SENSOR = "engine_temperature_sensor";

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
}
