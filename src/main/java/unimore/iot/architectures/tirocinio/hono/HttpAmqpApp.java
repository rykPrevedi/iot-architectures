package unimore.iot.architectures.tirocinio.hono;

import com.google.gson.Gson;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import kong.unirest.Unirest;
import org.eclipse.hono.application.client.DownstreamMessage;
import org.eclipse.hono.application.client.MessageConsumer;
import org.eclipse.hono.application.client.amqp.AmqpApplicationClient;
import org.eclipse.hono.application.client.amqp.AmqpMessageContext;
import org.eclipse.hono.application.client.amqp.ProtonBasedApplicationClient;
import org.eclipse.hono.client.ServiceInvocationException;
import org.eclipse.hono.client.amqp.config.ClientConfigProperties;
import org.eclipse.hono.client.amqp.connection.HonoConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.Constants.HonoConstants;
import unimore.iot.architectures.tirocinio.hono.devices.mqtt.model.MessageDescriptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static unimore.iot.architectures.tirocinio.hono.HttpProvisioningManagementApp.getDeviceByTenant;

/**
 * @author Riccardo Prevedi
 * @created 23/02/2023 - 17:14
 * @project architectures-iot
 */

public class HttpAmqpApp {
    private static final Logger LOG = LoggerFactory.getLogger(HttpAmqpApp.class);
    public static final String HONO_CLIENT_USER = "consumer@HONO";
    public static final String HONO_CLIENT_PASSWORD = "verysecret";
    private static final String deviceDRMApi = "/v1/devices/";
    private static final int RECONNECT_ATTEMPTS = 1;
    private AmqpApplicationClient client;   // An AMQP 1.0 based client that supports Hono's north bound operations
    private final Vertx vertx;
    private final ClientConfigProperties config;
    private MessageConsumer telemetryConsumer;

    private static Buffer temperatureValue;
    private final String subDeviceId = "mqtt-consumer-device";
    private static final String COMMAND_SEND_TEMPERATURE = "temperature";

    private final List<Double> temperatureValueList;

    public HttpAmqpApp() {
        vertx = Vertx.vertx();
        config = new ClientConfigProperties();
        config.setHost(HonoConstants.HONO_HOST);
        config.setPort(HonoConstants.HONO_AMQP_CONSUMER_PORT);
        config.setUsername(HONO_CLIENT_USER);
        config.setPassword(HONO_CLIENT_PASSWORD);
        config.setReconnectAttempts(RECONNECT_ATTEMPTS);

        temperatureValueList = new ArrayList<>();
    }

    public static void main(String[] args) {
        String baseUrl = String.format("http://%s:%d",
                HonoConstants.HONO_HOST,
                HonoConstants.HONO_HTTP_DEVICE_REGISTRY_PORT);
        Unirest.config().defaultBaseUrl(baseUrl);

        getDeviceByTenant(deviceDRMApi, HonoConstants.MY_TENANT_ID);

        HttpAmqpApp amqpApp = new HttpAmqpApp();
        amqpApp.connect();
    }

    private void sendOneWayCommandToDevice(DownstreamMessage<AmqpMessageContext> downstreamMessage) {
        temperatureValue = downstreamMessage.getPayload();
        client.sendOneWayCommand(HonoConstants.MY_TENANT_ID, subDeviceId, COMMAND_SEND_TEMPERATURE, temperatureValue)
                .onSuccess(new Handler<Void>() {
                    @Override
                    public void handle(Void status) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully sent one-way command payload: [{}] ", temperatureValue);
                        }
                    }
                })
                .onFailure(new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable t) {
                        if (t instanceof ServiceInvocationException) {
                            int errorCode = ((ServiceInvocationException) t).getErrorCode();
                            LOG.debug("One-way command was replied with error code [{}].", errorCode);
                        } else {
                            LOG.debug("Could not send one-way command : {}.", t.getMessage());
                        }
                    }
                });
    }


    /**
     * Parse the received MQTT messages into a MessageDescriptor object or null in case of error
     *
     * @param bufferPayload the Vert.x Buffer
     * @return the parsed MessageDescriptor object or null in case or error.
     */
    private MessageDescriptor parseJson(Buffer bufferPayload) {
        try {
            Gson gson = new Gson();
            return gson.fromJson(String.valueOf(bufferPayload), MessageDescriptor.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * The content-type is required if the PAYLOAD is EMPTY otherwise the message will be simply ignored
     * application/octet-stream is the default if no content-type is specified in the metadata
     *
     * @param dsMessage the downstream message whose payload content-type you want
     */
    private void checkDownStreamMessageContentType(DownstreamMessage<AmqpMessageContext> dsMessage) {

        final String octetStream = "application/octet-stream";
        final String json = "application/json";
        final String textPlain = "text/plain";

        String contentType = dsMessage.getContentType();

        switch (contentType) {
            case json -> {
                MessageDescriptor msgDescriptor = parseJson(dsMessage.getPayload());
                if (msgDescriptor != null) {
                    temperatureValueList.add(msgDescriptor.getValue());
                    System.out.println(getTemperatureValueList());
                } else {
                    LOG.info("Message Received - {} Message Received: {}", dsMessage.getDeviceId(), dsMessage.getPayload());
                }
            }
            case textPlain -> {
                temperatureValueList.add(Double.valueOf(dsMessage.getPayload().toString()));
                System.out.println(getTemperatureValueList());
            }
            default -> {
                LOG.info("Message with content-type : {} Its value is not considered !", octetStream);
                LOG.info("Message Received - {} Message Received: {}", dsMessage.getDeviceId(), dsMessage.getPayload());
            }
        }
    }


    /**
     * Handler method for a Message from Hono that was received as telemetry data.
     * <p>
     * <p>
     * The tenant, the device, the payload, the content-type, the creation-time and the application properties
     * will be logged.
     *
     * @param downstreamMessage The message that was received.
     */
    private void handleTelemetryMessage(DownstreamMessage<AmqpMessageContext> downstreamMessage) {
        LOG.info("received telemetry data [tenant: {}, device: {}, content-type: {}]: [{}].",
                downstreamMessage.getTenantId(),
                downstreamMessage.getDeviceId(),
                downstreamMessage.getContentType(),
                downstreamMessage.getPayload());

        checkDownStreamMessageContentType(downstreamMessage);
    }

    private void connect() {
        HonoConnection connection = HonoConnection.newConnection(vertx, config);
        connection
                .connect()
                .onSuccess(c -> {
                    LOG.info("The Client {} is connect to the AMQP messaging router!", HONO_CLIENT_USER);
                    LOG.info("Ready for Hono operations : Telemetry ... Command ... ");
                    start(c);
                })
                .onFailure(t -> LOG.error(" {} ", t.getMessage()));
    }

    private void start(HonoConnection connection) {
        // ProtonBasedApplicationClient Implements AmqpApplicationClient.
        // A vertx-proton based client that supports Hono's north bound operations to send commands and receive telemetry, event and command response messages.
        client = new ProtonBasedApplicationClient(connection);
        createTelemetryConsumer();
    }


    private void createTelemetryConsumer() {
        client.createTelemetryConsumer(HonoConstants.MY_TENANT_ID,
                new Handler<DownstreamMessage<AmqpMessageContext>>() {
                    @Override
                    public void handle(DownstreamMessage<AmqpMessageContext> msg) {
                        handleTelemetryMessage(msg);
                        //sendOneWayCommandToDevice(msg);
                    }
                }, new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable t) {
                        LOG.error("telemetry consumer closed by remote " + t);
                    }
                }).onSuccess(messageConsumer -> telemetryConsumer = messageConsumer);
    }

    public List<Double> getTemperatureValueList() {
        return temperatureValueList;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("HttpAmqpApp{");
        sb.append("temperatureValueList=").append(temperatureValueList);
        sb.append('}');
        return sb.toString();
    }
}
