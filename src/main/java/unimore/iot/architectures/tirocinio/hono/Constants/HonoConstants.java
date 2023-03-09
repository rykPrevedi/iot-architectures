package unimore.iot.architectures.tirocinio.hono.Constants;

/**
 * @author Riccardo Prevedi
 * @created 23/02/2023 - 11:22
 * @project architectures-iot
 */

public class HonoConstants {

    // (k8s-node-1) IP address of Kubernetes node where HONO is running
    public static final String HONO_HOST = "192.168.181.17";


    /**
     * SERVICE-DEVICE-REGISTRY
     *
     * Device registry port where applications can register the device and tenant and set the device password.
     * Through the Device Registry Management API
     */
    // (service name: eclipse-hono-service-device-registry-ext)
    // external Port where the endpoint can be reached via the HTTP protocol communication
    public static final int HONO_HTTP_DEVICE_REGISTRY_PORT = 30274;

    // external Port where the endpoint can be reached via the HTTPS protocol communication
    public static final int HONO_HTTPS_DEVICE_REGISTRY_PORT = 30054;

    /**
     * AMQP-MESSAGING-NETWORK-ROUTER
     *
     * Port of the AMQP network where consumers can receive data (in the standard setup this is the port
     * of the qdrouter).
     */
    // (service name: eclipse-hono-dispatch-router-ext)
    // external Port where the endpoint can be reached via the AMQP protocol communication
    public static final int HONO_AMQP_CONSUMER_PORT = 30546;

    // external Port where the endpoint can be reached via the AMQPS protocol communication
    public static final int HONO_AMQPS_CONSUMER_PORT = 30974;

    /**
     * MQTT-HONO-PROTOCOL-ADAPTER
     *
     * Port of the MQTT protocol adapter where the device can Publish messages or Subscribe to a specific topic
     */
    // (service name: eclipse-hono-adapter-mqtt)
    // external Port where the endpoint can be reached via the MQTT protocol communication
    public static final int HONO_MQTT_ADAPTER_PORT = 30124;

    // external Port where the endpoint can be reached via the MQTT protocol communication
    public static final int HONO_SECURE_MQTT_ADAPTER_PORT = 30267;

    /**
     * AMQP-HONO-PROTOCOL-ADAPTER
     *
     * Port of the AMQP protocol adapter where the device supporting AMQP 1.0
     * can Publish messages to Eclipse Hono Telemetry, Event and Command & Control Endpoints
     */
    // (service name: eclipse-hono-adapter-amqp)
    // external Port where the endpoint can be reached via the AMQP protocol communication
    public static final int HONO_AMQP_ADAPTER_PORT = 30269;

    // external Port where the endpoint can be reached via the AMQPS protocol communication
    public static final int HONO_AMQPS_ADAPTER_PORT = 31206;

    /**
     * Hono is designed to structure
     * the set of all internally managed data and data streams into strictly isolated subsets, called tenant.
     * Isolation is essential
     * for enabling a scalable distributed architecture
     * to handle independent subsets as if each subset had its own installation
     */
    public static final String MY_TENANT_ID = "mytenant";

    /**
     * For devices signaling that they remain connected for an indeterminate amount of time, a command is
     * periodically sent to the device after the following number of seconds elapsed.
     */
    public static final int COMMAND_INTERVAL_FOR_DEVICES_CONNECTED_WITH_UNLIMITED_EXPIRY =
            Integer.parseInt(System.getProperty("command.repetition.interval", "5"));

    private HonoConstants() { // prevent instantiation
    }

}
