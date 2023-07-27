package unimore.iot.architectures.tirocinio.hono.devices.amqp;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.eclipse.hono.client.ServiceInvocationException;
import org.eclipse.hono.client.amqp.config.ClientConfigProperties;
import org.eclipse.hono.client.amqp.connection.HonoConnection;
import org.eclipse.hono.client.device.amqp.AmqpAdapterClient;
import org.eclipse.hono.util.QoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.Constants.HonoConstants;
import unimore.iot.architectures.tirocinio.hono.devices.mqtt.JsonTemperatureProducer;

/**
 * @author Riccardo Prevedi
 * @created 09/03/2023 - 16:59
 * @project architectures-iot
 */

public class AmqpDevice {
    private static final Logger LOG = LoggerFactory.getLogger(JsonTemperatureProducer.class);
    private static final String TENANT_ID = "mytenant";
    private static final String AUTH_ID = "device-amqp";
    private static final String PASSWORD = "hono-secret";
    private static final int RECONNECT_ATTEMPTS = 3;
    private static final int CONNECTION_TIMEOUT = 1000 * 5;

    private final ClientConfigProperties config = new ClientConfigProperties();
    private final Vertx vertx = Vertx.vertx();

    // A Vert.x based client for interacting with Hono's AMPQ adapter
    private AmqpAdapterClient client;

    public AmqpDevice() {
        config.setHost(HonoConstants.HONO_HOST);
        config.setPort(HonoConstants.HONO_AMQP_ADAPTER_PORT);
        config.setUsername(AUTH_ID + "@" + TENANT_ID);
        config.setPassword(PASSWORD);
        config.setReconnectAttempts(RECONNECT_ATTEMPTS);
    }

    public static void main(String[] args) {
        AmqpDevice amqpDevice = new AmqpDevice();
        amqpDevice.connect();
    }

    private void connect() {
        HonoConnection connection = HonoConnection.newConnection(vertx, config);
        connection.connect()
                .onSuccess(connection1 -> {
                    LOG.info("connected to the AMPQ Adapter [{}]", connection1.getRemoteContainerId());
                    startDevice(connection1);
                    vertx.setTimer(CONNECTION_TIMEOUT, l -> disconnectClient(connection));
                })
                .onFailure(t -> {
                    LOG.error("Failed to establish connection: ", t);
                    System.exit(1);
                });
    }

    private void startDevice(HonoConnection connection) {
        client = AmqpAdapterClient.create(connection);

        sendTelemetryMessageWithQos0();

        //vertx.setTimer(1000, l -> sendTelemetryMessageWithQos1());
    }

    private void sendTelemetryMessageWithQos0() {

        String payload = "42";
        client.sendTelemetry(QoS.AT_MOST_ONCE, Buffer.buffer(payload), "text/plain", null, null, null)
                .onSuccess(delivery -> LOG.info("Telemetry message with QoS 'AT_MOST_ONCE' sent: {}", payload))
                .onFailure(t -> LOG.error("Sending telemetry message with QoS 'AT_MOST_ONCE' failed: " + t));
    }

    private void sendTelemetryMessageWithQos1() {

        JsonObject jsonPayload = new JsonObject().put("temperature", 42);
        client.sendTelemetry(QoS.AT_LEAST_ONCE, jsonPayload.toBuffer(), "application/json", null, null, null)
                .onSuccess(delivery -> LOG.info("Telemetry message with QoS 'AT_MOST_ONCE' sent: {}", jsonPayload))
                .onFailure(t -> {
                    String hint = "";
                    if (ServiceInvocationException.extractStatusCode(t) == 503) {
                        hint = " (is there a consumer connected?)";
                    }
                    LOG.error("Sending telemetry message with QoS 'AT_LEAST_ONCE' failed: " + t + hint);
                });
    }

    private void disconnectClient(HonoConnection connection) {
        connection.disconnect();
        System.exit(0);
    }
}
