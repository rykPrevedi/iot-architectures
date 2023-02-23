package unimore.iot.architectures.tirocinio.hono.Constants;

/**
 * @author Riccardo Prevedi
 * @created 23/02/2023 - 11:22
 * @project architectures-iot
 */

public class HonoConstants {

    // (k8s-node-1) IP address of Kubernetes node where HONO is running
    public static final String HONO_HOST = "192.168.181.17";

    // (eclipse-hono-service-device-registry-ext)
    // external Port where the endpoint can be reached by the HTTP protocol communication
    public static final int HONO_HTTP_DEVICE_REGISTRY_PORT = 31735;

    // (eclipse-hono-service-device-registry-ext)
    // external Port where the endpoint can be reached by the HTTPS protocol communication
    public static final int HONO_HTTPS_DEVICE_REGISTRY_PORT = 30659;

    /**
     * Hono is designed to structure
     * the set of all internally managed data and data streams into strictly isolated subsets, called tenant.
     * Isolation is essential
     * for enabling a scalable distributed architecture
     * to handle independent subsets as if each subset had its own installation
     */
    public static final String MY_TENANT_ID = "myTenant";

    private HonoConstants() { // prevent instantiation
    }

}
