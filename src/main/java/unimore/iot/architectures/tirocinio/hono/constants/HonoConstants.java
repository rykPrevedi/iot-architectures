package unimore.iot.architectures.tirocinio.hono.constants;

/**
 * @author Riccardo Prevedi
 * @created 23/02/2023 - 11:22
 * @project architectures-iot
 */

public class HonoConstants {

    // (k8s-node-1) IP address of Kubernetes node where HONO is running
    public static final String HONO_HOST = "192.168.56.18";


    /**
     * SERVICE-DEVICE-REGISTRY
     * (svc name: eclipse-hono-service-device-registry-ext)
     * *
     * Port of the Device Registry where applications can register devices and tenants and update the device credentials.
     * Through the Device Registry Management API
     */

    // external Port where the endpoint can be reached via the HTTP protocol communication
    public static final int HONO_HTTP_DEVICE_REGISTRY_PORT = 30274;
    // external Port where the endpoint can be reached via the HTTPS protocol communication
    public static final int HONO_HTTPS_DEVICE_REGISTRY_PORT = 31399;

    /**
     * AMQP-MESSAGING-NETWORK-ROUTER
     * (svc name: eclipse-hono-dispatch-router-ext)
     * *
     * Port of the AMQP network where consumers can receive data
     * (in the standard setup this is the port of the qdrouter or Qpid Dispatch Router).
     */

    // external Port where the endpoint can be reached via the AMQP protocol communication
    public static final int HONO_AMQP_CONSUMER_PORT = 31453;
    // external Port where the endpoint can be reached via the AMQPS protocol communication
    public static final int HONO_AMQPS_CONSUMER_PORT = 30670;

    /**
     * MQTT-HONO-PROTOCOL-ADAPTER
     * (svc name: eclipse-hono-adapter-mqtt)
     * *
     * Port of the MQTT protocol adapter where the device can Publish messages or Subscribe to a specific topic
     */
    // external Port where the endpoint can be reached via the MQTT protocol communication
    public static final int HONO_MQTT_ADAPTER_PORT = 30124;
    // external Port where the endpoint can be reached via the MQTT protocol communication
    public static final int HONO_SECURE_MQTT_ADAPTER_PORT = 32176;

    /**
     * AMQP-HONO-PROTOCOL-ADAPTER
     * (svc name: eclipse-hono-adapter-amqp)
     * *
     * Port of the AMQP protocol adapter where the device supporting AMQP 1.0
     * can Publish messages to Eclipse Hono Telemetry, Event and Command & Control Endpoints
     */
    // external Port where the endpoint can be reached via the AMQP protocol communication
    public static final int HONO_AMQP_ADAPTER_PORT = 30269;
    // external Port where the endpoint can be reached via the AMQPS protocol communication
    public static final int HONO_AMQPS_ADAPTER_PORT = 31294;

    /**
     * HTTP-HONO-PROTOCOL-ADAPTER
     * (svc name: eclipse-hono-adapter-http)
     * *
     * Port of the HTTP protocol adapter where the device can connect to in order to send or receive messages
     *
     */
    // external Port where the endpoint can be reached via the HTTP protocol communication
    public static final int HONO_HTTP_ADAPTER_PORT = 30516;
    // external Port where the endpoint can be reached via the HTTPS protocol communication
    public static final int HONO_HTTPS_ADAPTER_PORT = 31398;



    /**
     * Hono is designed to structure
     * the set of all internally managed data and data streams into strictly isolated subsets, called tenant.
     * Isolation is essential
     * for enabling a scalable distributed architecture
     * to handle independent subsets as if each subset had its own installation
     */

    // The Smart Building "tenant-floor-area"
    public static final String MY_TENANT_ID = "tenant-00-a1";

    // Device universal password (not to be used in production)
    public static final String devicePassword = "password";

    // AMQP ------------------------------------------------------------------------

    //TODO:
    // private static final String amqpDeviceAuthId = "device-amqp";
    public static final String amqpCommandConsumerAuthId = "amqp-command-consumer";

    // MQTT ------------------------------------------------------------------------

    public static final String mqttDeviceAuthId = "device-mqtt";

    // HTTP ------------------------------------------------------------------------

    public static final String httpDeviceAuthId = "device-http";


    /**
     * For devices signaling that they remain connected for an indeterminate amount of time, a command is
     * periodically sent to the device after the following number of seconds elapsed.
     */
    public static final int COMMAND_INTERVAL_FOR_DEVICES_CONNECTED_WITH_UNLIMITED_EXPIRY =
            Integer.parseInt(System.getProperty("command.repetition.interval", "5"));

    private HonoConstants() { // prevent instantiation
    }

}
