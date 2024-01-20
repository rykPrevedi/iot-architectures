package unimore.iot.architectures.tirocinio.hono.businessapplications;

import com.google.gson.Gson;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import kong.unirest.Unirest;
import org.eclipse.hono.application.client.DownstreamMessage;
import org.eclipse.hono.application.client.MessageConsumer;
import org.eclipse.hono.application.client.TimeUntilDisconnectNotification;
import org.eclipse.hono.application.client.amqp.AmqpApplicationClient;
import org.eclipse.hono.application.client.amqp.AmqpMessageContext;
import org.eclipse.hono.application.client.amqp.ProtonBasedApplicationClient;
import org.eclipse.hono.client.ServiceInvocationException;
import org.eclipse.hono.client.amqp.config.ClientConfigProperties;
import org.eclipse.hono.client.amqp.connection.HonoConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.constants.HonoConstants;
import unimore.iot.architectures.tirocinio.hono.model.MessageDescriptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static unimore.iot.architectures.tirocinio.hono.devicesprovision.HonoHttpProvisionerBase.getDeviceByTenant;

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
    private static final String COMMAND_SEND_TEMPERATURE = "temperature";

    private final List<Double> temperatureValueList;

    /**
     * A map holding a handler to cancel a timer that was started to send commands periodically to a device.
     * Only affects devices that use a connection oriented protocol like MQTT.
     */
    private final Map<String, Handler<Void>> periodicCommandSenderTimerCancelerMap = new HashMap<>();

    /**
     * A map holding the last reported notification for a device being connected. Will be emptied as soon as the
     * notification is handled.
     * Only affects devices that use a connection oriented protocol like MQTT.
     */
    private final Map<String, TimeUntilDisconnectNotification> pendingTtdNotification = new HashMap<>();


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
                HonoConstants.HONO_HTTPS_DEVICE_REGISTRY_PORT);
        Unirest.config().defaultBaseUrl(baseUrl);

        getDeviceByTenant(deviceDRMApi, HonoConstants.MY_TENANT_ID);

        HttpAmqpApp amqpApp = new HttpAmqpApp();
        amqpApp.connect();
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
        createEventConsumer();

    }

    /**
     * The app try to connect to the specific AMQP 1.0 Endpoint event/TENANT
     * Only to handle the Time Until Disconnect Notification sending through an Event type message
     */
    private void createEventConsumer() {
        client.createEventConsumer(HonoConstants.MY_TENANT_ID,
                        msg -> {
                            // handle command readiness notification
                            msg.getTimeUntilDisconnectNotification().ifPresent(this::handleCommandReadinessNotification);
                        },
                        t -> LOG.error("telemetry consumer closed by remote " + t))
                .onSuccess(messageConsumer -> telemetryConsumer = messageConsumer);
    }


    /**
     * The app try to connect to the specific AMQP 1.0 Endpoint telemetry/TENANT
     */
    private void createTelemetryConsumer() {
        client.createTelemetryConsumer(HonoConstants.MY_TENANT_ID,
                new Handler<DownstreamMessage<AmqpMessageContext>>() {
                    @Override
                    public void handle(DownstreamMessage<AmqpMessageContext> msg) {
                        msg.getTimeUntilDisconnectNotification().ifPresent(new Consumer<TimeUntilDisconnectNotification>() {
                            @Override
                            public void accept(TimeUntilDisconnectNotification notification) {
                                handleCommandReadinessNotification(notification);
                                //return null;
                            }
                        });
                        handleTelemetryMessage(msg);
                    }
                }, new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable t) {
                        LOG.error("telemetry consumer closed by remote " + t);
                    }
                }).onSuccess(messageConsumer -> telemetryConsumer = messageConsumer);
    }


    /**
     * Handler method for a device ready for command notification (by an explicit event or contained implicitly in another message).
     * For notifications with a positive ttd value (as usual for request-response protocols),
     * the code creates a simple command in JSON.
     * For notifications signaling a connection oriented protocol,
     * the handling is delegated to {@link #handlePermanentlyConnectedCommandReadinessNotification(TimeUntilDisconnectNotification)}.
     *
     * @param notification The notification containing the tenantId, deviceId and the Instant (that defines until when this notification is valid).
     */
    private void handleCommandReadinessNotification(TimeUntilDisconnectNotification notification) {
        if (notification.getTtd() <= 0) {
            handlePermanentlyConnectedCommandReadinessNotification(notification);
        } else {
            LOG.info("Device is ready to receive a command : [{}].", notification);
            sendCommand(notification);
        }
    }

    private void handlePermanentlyConnectedCommandReadinessNotification(TimeUntilDisconnectNotification notification) {

        String keyForDevice = notification.getTenantAndDeviceId();

        TimeUntilDisconnectNotification previousNotification = pendingTtdNotification.get(keyForDevice);
        if (previousNotification != null) {
            if (notification.getCreationTime().isAfter(previousNotification.getCreationTime())) {
                LOG.info("Set new ttd value [{}] of notification for [{}]",
                        notification.getTtd(), notification.getTenantAndDeviceId());
                pendingTtdNotification.put(keyForDevice, notification);
            } else {
                LOG.trace("Received notification for [{}] that was already superseded by newer [{}]",
                        notification, previousNotification);
            }
        } else {
            pendingTtdNotification.put(keyForDevice, notification);
            // there was no notification available already, so start a handler now
            vertx.setTimer(1000, timerId -> {
                LOG.info("Handle device notification for: [{}].", notification.getTenantAndDeviceId());
                // now take the notification from the pending map and handle it
                TimeUntilDisconnectNotification notificationToHandle = pendingTtdNotification.remove(keyForDevice);
                if (notificationToHandle != null) {
                    if (notificationToHandle.getTtd() == -1) {
                        LOG.info("Device notified as being ready to receive a command until further notice : [{}].", notificationToHandle);

                        // cancel a still existing timer for this device (if found)
                        cancelPeriodicCommandSender(notification);
                        // immediately send the first command
                        sendCommand(notificationToHandle);

                        // for devices that stay connected, start a periodic timer now that repeatedly sends a command
                        // to the device
                        vertx.setPeriodic(
                                (long) HonoConstants.COMMAND_INTERVAL_FOR_DEVICES_CONNECTED_WITH_UNLIMITED_EXPIRY * 1000,
                                id -> {
                                    sendCommand(notificationToHandle);
                                    // register a canceler for this timer directly after it was created
                                    setPeriodicCommandSenderTimerCanceler(id, notification);
                                });
                    } else {
                        LOG.info("Device notified as not being ready to receive a command (anymore) : [{}].", notification);
                        cancelPeriodicCommandSender(notificationToHandle);
                        LOG.debug("Device will not receive further commands : [{}].", notification.getTenantAndDeviceId());
                    }
                }
            });
        }
    }

    /**
     * Sends a command to the device for which a {@link TimeUntilDisconnectNotification} was received.
     *
     * @param notification The notification that was received for the device.
     */
    private void sendCommand(TimeUntilDisconnectNotification notification) {
        sendOneWayCommandToAdapter(notification.getTenantId(), notification.getDeviceId(), notification);
    }

    private void cancelPeriodicCommandSender(TimeUntilDisconnectNotification notification) {
        if (isPeriodicCommandSenderActiveForDevice(notification)) {
            LOG.debug("Cancelling periodic sender for {}", notification.getTenantAndDeviceId());
            periodicCommandSenderTimerCancelerMap.get(notification.getTenantAndDeviceId()).handle(null);
        } else {
            LOG.debug("Wanted to cancel periodic sender for {}, but could not find one",
                    notification.getTenantAndDeviceId());
        }
    }

    private void setPeriodicCommandSenderTimerCanceler(Long timerId, TimeUntilDisconnectNotification ttdNotification) {
        this.periodicCommandSenderTimerCancelerMap.put(ttdNotification.getTenantAndDeviceId(), v -> {
            vertx.cancelTimer(timerId);
            periodicCommandSenderTimerCancelerMap.remove(ttdNotification.getTenantAndDeviceId());
        });
    }

    private boolean isPeriodicCommandSenderActiveForDevice(TimeUntilDisconnectNotification notification) {
        return periodicCommandSenderTimerCancelerMap.containsKey(notification.getTenantAndDeviceId());
    }


    private void sendOneWayCommandToAdapter(String tenantId, String deviceId, TimeUntilDisconnectNotification ttdNotification) {

        Buffer commandBuffer = buildOneWayCommandPayload();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending one-way command [{}] to [{}].",
                    COMMAND_SEND_TEMPERATURE,
                    ttdNotification.getTenantAndDeviceId());
        }

        client.sendOneWayCommand(tenantId, deviceId, COMMAND_SEND_TEMPERATURE, commandBuffer)
                .onSuccess(status -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Successfully send one-way command payload: [{}] and receive status [{}].",
                                commandBuffer, status);
                    }
                })
                .onFailure(t -> {
                    if (t instanceof ServiceInvocationException) {
                        int errorCode = ((ServiceInvocationException) t).getErrorCode();
                        LOG.debug("One-way command was replied with error code [{}].", errorCode);
                    } else {
                        LOG.debug("Could not send one-way command : {}.", t.getMessage());
                    }
                });

    }

    private Buffer buildOneWayCommandPayload() {
        JsonObject jsonCmd = new JsonObject().put("temperature avg", getTemperatureAverage());
        return Buffer.buffer(jsonCmd.encodePrettily());
    }

    private double getTemperatureAverage() {
        return temperatureValueList.stream()
                .mapToDouble(value -> value)
                .average().orElse(0);
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
     * ATTENTION:
     * The content-type is required if the PAYLOAD is EMPTY otherwise the message will be simply ignored by Hono
     * application/octet-stream is the default if no content-type is specified in the metadata
     *
     * @param dsMessage the downstream message whose payload content-type you want
     */
    private void checkDownStreamMessageContentType(DownstreamMessage<AmqpMessageContext> dsMessage) {

        final String octetStream = "application/octet-stream";
        final String json = "application/json";
        final String textPlain = "text/plain";

        switch (dsMessage.getContentType()) {
            case json -> {
                MessageDescriptor msgDescriptor = parseJson(dsMessage.getPayload());
                if (msgDescriptor != null) {
                    temperatureValueList.add(msgDescriptor.getValue());
                } else {
                    LOG.info("Message Received - {} Message Received: {}", dsMessage.getDeviceId(), dsMessage.getPayload());
                }
            }
            case textPlain -> temperatureValueList.add(Double.valueOf(dsMessage.getPayload().toString()));
            default -> {
                LOG.info("content-type : {} the message value is NOT considered !", octetStream);
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


    public List<Double> getTemperatureValueList() {
        return temperatureValueList;
    }

    @Override
    public String toString() {
        String sb = "HttpAmqpApp{" + "temperatureValueList=" + temperatureValueList +
                '}';
        return sb;
    }
}
