package unimore.iot.architectures.tirocinio.hono.devices.amqp;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.proton.ProtonDelivery;
import kong.unirest.Unirest;
import kong.unirest.json.JSONArray;
import org.eclipse.hono.client.amqp.config.ClientConfigProperties;
import org.eclipse.hono.client.amqp.connection.HonoConnection;
import org.eclipse.hono.client.device.amqp.AmqpAdapterClient;
import org.eclipse.hono.util.QoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.Constants.HonoConstants;


/**
 * @author Riccardo Prevedi
 * @created 10/03/2023 - 10:53
 * @project architectures-iot
 */

public class UnAuthAmqpDevice {
    private static final Logger LOG = LoggerFactory.getLogger(UnAuthAmqpDevice.class);
    private static final String AUTH_ID = "device-amqp";
    private static final String PASSWORD = "hono-secret";
    private static final String DEVICE_ID = "unAuth-amqp-device";
    private static final int CONNECTION_TIME = 1000 * 5;    // milliseconds
    private static final int RECONNECT_ATTEMPTS = 3;

    private static String tenantId;
    private static String floor = "first";

    private final ClientConfigProperties config = new ClientConfigProperties();
    private final static Vertx vertx = Vertx.vertx();

    private AmqpAdapterClient amqpAdapterClient;

    public UnAuthAmqpDevice() {
        config.setHost(HonoConstants.HONO_HOST);
        config.setPort(HonoConstants.HONO_AMQP_ADAPTER_PORT);
        config.setUsername(AUTH_ID + "@" + tenantId);
        config.setPassword(PASSWORD);
        config.setReconnectAttempts(RECONNECT_ATTEMPTS);
    }

    public static void main(String[] args) {
        String baseUrl = String.format("http://%s:%d",
                HonoConstants.HONO_HOST,
                HonoConstants.HONO_HTTP_DEVICE_REGISTRY_PORT);
        Unirest.config().defaultBaseUrl(baseUrl);

        // check for the "ext": {"floor": "first"}
        checkTenant(floor);

        UnAuthAmqpDevice unAuthAmqpDevice = new UnAuthAmqpDevice();
        unAuthAmqpDevice.connect();
    }

    private static void checkTenant(String floorNumber) {

        // get the list of all available tenants
        JSONArray tenantArray =
                Unirest
                        .get("/v1/tenants/")
                        .asJson()
                        .getBody()
                        .getObject()
                        .getJSONArray("result");

        System.out.println(tenantArray);

        // now extract the "ext" member values
        for (int i = 0; i < tenantArray.length(); i++) {
            if (tenantArray.getJSONObject(i).optJSONObject("ext") != null) {
                if (tenantArray.getJSONObject(i).getJSONObject("ext").get("floor").equals(floorNumber)) {
                    tenantId = tenantArray.getJSONObject(i).get("id").toString();
                    System.out.println(tenantArray.getJSONObject(i).get("id").toString());
                }
            }
        }
    }

    private void connect() {
        HonoConnection connection = HonoConnection.newConnection(vertx, config);
        connection.connect()
                .onSuccess(new Handler<HonoConnection>() {
                    @Override
                    public void handle(HonoConnection connection) {
                        LOG.info("Connected to the AMPQ Adapter [{}]", connection.getRemoteContainerId());
                        startDevice(connection);
                        vertx.setTimer(CONNECTION_TIME, l -> disconnectClient(connection));
                    }
                })
                .onFailure(new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable t) {
                        LOG.error("Failed to establish connection: ", t);
                    }
                });
    }

    private void startDevice(HonoConnection connection) {
        amqpAdapterClient = AmqpAdapterClient.create(connection);
        sendTelemetryMessageWithQos0();
    }

    // Send message from an Unregistered Device
    private void sendTelemetryMessageWithQos0() {
        String payload = "42";
        amqpAdapterClient.sendTelemetry(QoS.AT_MOST_ONCE,
                        Buffer.buffer(payload),
                        "text/plain",
                        tenantId,
                        DEVICE_ID,
                        null)
                .onSuccess(new Handler<ProtonDelivery>() {
                    @Override
                    public void handle(ProtonDelivery delivery) {
                        LOG.info("Telemetry message with QoS 'AT_MOST_ONCE' sent: {}", payload);
                    }
                })
                .onFailure(new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable t) {
                        LOG.error("Sending telemetry message with QoS 'AT_MOST_ONCE' failed: " + t);
                    }
                });
    }

    private void disconnectClient(HonoConnection connection) {
        connection.disconnect();
        System.exit(0);
    }
}
