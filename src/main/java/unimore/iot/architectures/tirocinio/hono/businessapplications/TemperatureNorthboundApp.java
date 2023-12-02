package unimore.iot.architectures.tirocinio.hono.businessapplications;

import com.google.gson.Gson;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
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
import unimore.iot.architectures.tirocinio.hono.Constants.HonoConstants;
import unimore.iot.architectures.tirocinio.hono.devices.mqtt.model.MessageDescriptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;


/**
 * This Northbound Business Application allows to
 *
 * 1. Receive temperature data from devices belonging to a tenant automatically configured in
 *    {@link unimore.iot.architectures.tirocinio.hono.BusinessApplicationEngine}
 * 2. Calculate the average of all temperature values received both in Json and text-plain format
 *    while octet-stream will be simply ignored
 * 3. Using device notifications automatically sends commands containing the average temperature
 *    to all devices that subscribe to the command topic.
 *
 *
 * @author Riccardo Prevedi
 * @created 05/03/2023 - 18:45
 * @project architectures-iot
 */

public class TemperatureNorthboundApp {
    private static final Logger LOG = LoggerFactory.getLogger(TemperatureNorthboundApp.class);
    private static final String HONO_CLIENT_USER = "consumer@HONO";
    private static final String HONO_CLIENT_PASSWORD = "verysecret";
    private static final int RECONNECT_ATTEMPTS = 2;
    private String tenant;

    private final AmqpApplicationClient client;
    private final Vertx vertx = Vertx.vertx();  // embedded Vertx instance

    private MessageConsumer telemetryConsumer;
    private MessageConsumer eventConsumer;
    private static final String COMMAND_SEND_TEMPERATURE = "temperature";

    /**
     * A list that manages temperature data from devices that belong to the tenant
     */
    private final List<Double> temperatureValueList = new ArrayList<>();

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


    public TemperatureNorthboundApp() {
        client = createApplicationClient();
    }

    /**
     * The consumer needs one connection to the AMQP 1.0 messaging network from which it can consume data.
     * <p>
     * The client for receiving data is instantiated here.
     * <p>
     */
    private AmqpApplicationClient createApplicationClient() {
        ClientConfigProperties props = new ClientConfigProperties();
        props.setLinkEstablishmentTimeout(5000L);   // milliseconds, the default is 1000L
        props.setHost(HonoConstants.HONO_HOST);
        props.setPort(HonoConstants.HONO_AMQP_CONSUMER_PORT);
        props.setUsername(HONO_CLIENT_USER);
        props.setPassword(HONO_CLIENT_PASSWORD);
        props.setReconnectAttempts(RECONNECT_ATTEMPTS);
        return new ProtonBasedApplicationClient(HonoConnection.newConnection(vertx, props));
    }

    /**
     * Start the application client and set the message handling method to treat data that is received.
     */
    public void consumeData() {

        // Instantiate the CompletableFuture
        // The asynchronous Java Object
        CompletableFuture<AmqpApplicationClient> startup = new CompletableFuture<>();

        client.start()
                // When the client is started (fut1), execute this:
                .compose(v -> CompositeFuture.all(createEventConsumer(), createTelemetryConsumer()))
                .onSuccess(ok -> startup.complete(client))  // Completes the Future, All consumers are created
                .onFailure(startup::completeExceptionally); // At least one consumer creation failed

        try {
            startup.join();
            LOG.info("The consumer is ready for telemetry and event messages that belong to the tenant: {}", getTenant());
        } catch (CompletionException e) {
            LOG.error("{} consumer failed to start [{}:{}]",
                    "AMQP", HonoConstants.HONO_HOST, HonoConstants.HONO_AMQP_CONSUMER_PORT, e.getCause());
        }

        // TODO: implements client shutDown
    }

    /**
     * The app tries to connect to the specific AMQP 1.0 Endpoint event/TENANT
     * And then creates the message consumer that handles event messages and invokes the notification callback
     * {@link #handleCommandReadinessNotification(TimeUntilDisconnectNotification)} if the message indicates that it
     * stays connected for a specified time.
     *
     * @return A succeeded future if the creation was successful, a failed Future otherwise.
     */
    private Future<MessageConsumer> createEventConsumer() {
        return client.createEventConsumer(
                        tenant,
                        msg -> {
                            // handle command readiness notification
                            msg.getTimeUntilDisconnectNotification().ifPresent(this::handleCommandReadinessNotification);
                            handleEventMessage(msg);
                        },
                        cause -> LOG.error("event consumer closed by remote", cause))
                .onSuccess(consumer -> this.eventConsumer = consumer);
    }

    /**
     * The app tries to connect to the specific AMQP 1.0 Endpoint telemetry/TENANT
     * And then creates the message consumer that handles telemetry messages and invokes the notification callback
     * {@link #handleCommandReadinessNotification(TimeUntilDisconnectNotification)} if the message indicates that it
     * stays connected for a specified time.
     *
     * @return A succeeded future if the creation was successful, a failed Future otherwise.
     */
    private Future<MessageConsumer> createTelemetryConsumer() {
        return client.createTelemetryConsumer(
                        tenant,
                        msg -> {
                            // handle command readiness notification
                            msg.getTimeUntilDisconnectNotification().ifPresent(this::handleCommandReadinessNotification);
                            handleTelemetryMessage(msg);
                            checkTelemetryMessageContentType(msg);
                        },
                        cause -> LOG.error("telemetry consumer closed by remote", cause))
                .onSuccess(consumer -> this.telemetryConsumer = consumer);
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
            // ther was no notification available already, so start handler now
            vertx.setTimer(1000, timerId -> {
                LOG.info("Handle device notification for: [{}]", notification.getTenantAndDeviceId());
                // now take the notification from the pending map and handle it
                TimeUntilDisconnectNotification notificationToHandle = pendingTtdNotification.remove(keyForDevice);
                if (notificationToHandle != null) {
                    if (notificationToHandle.getTtd() == -1) {
                        LOG.info("Device notified as being ready to receive a command until further notice : [{}].", notificationToHandle);

                        // cancel a still existing timer for this device (if found)
                        cancelPeriodicCommandSender(notification);
                        // immediately send the first command
                        sendCommand(notificationToHandle);

                        // for devices that stay connected, start a periodic timer now that repeatly sendes a command
                        // to the device
                        vertx.setPeriodic((long) HonoConstants.COMMAND_INTERVAL_FOR_DEVICES_CONNECTED_WITH_UNLIMITED_EXPIRY * 1000,
                                id -> {
                                    sendCommand(notificationToHandle);
                                    setPeriodicCommandSenderTimerCanceler(id, notification);
                                });
                    } else {
                        LOG.info("Device notified as not being ready to receive a command (anymore) : [{}]", notification);
                        cancelPeriodicCommandSender(notificationToHandle);
                        LOG.debug("Device will not receive further commands : [{}]", notification.getTenantAndDeviceId());
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

    /**
     * ATTENTION:
     * The content-type is required if the PAYLOAD is EMPTY otherwise the message will be simply ignored by Hono
     * application/octet-stream is the default if no content-type is specified in the metadata
     *
     * @param dsMessage the downstream message whose payload content-type you want
     */
    private void checkTelemetryMessageContentType(DownstreamMessage<AmqpMessageContext> dsMessage) {

        final String octetStream = "application/octet-stream";
        final String json = "application/json";
        final String textPlain = "text/plain";

        switch (dsMessage.getContentType()) {
            case json -> {
                MessageDescriptor msgDescriptor = parseJson(dsMessage.getPayload());
                if (msgDescriptor != null) {
                    temperatureValueList.add(msgDescriptor.getValue());
                    LOG.debug("{}", getTemperatureValueList());
                } else {
                    LOG.info("Received Message from : {} - content : {}", dsMessage.getDeviceId(), dsMessage.getPayload());
                }
            }
            case textPlain -> {
                temperatureValueList.add(Double.valueOf(dsMessage.getPayload().toString()));
                LOG.debug("{}", getTemperatureValueList());
            }
            default -> {
                LOG.info("Received Message from : {} - content : {}", dsMessage.getDeviceId(), dsMessage.getPayload());
                LOG.info("content-type : {} the message value is NOT considered !", octetStream);
            }
        }
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
     * Send a one way command to the device for which a {@link TimeUntilDisconnectNotification} was received.
     * <p>
     * If the contained <em>ttd</em> is set to a value @gt; 0, the commandClient will be closed after a response
     * was received.
     * If the contained <em>ttd</em> is set to -1, the commandClient will remain open for further commands to be sent.
     *
     * @param ttdNotification The ttd notification that was received for the device.
     */
    private void sendOneWayCommandToAdapter(String tenantId, String deviceId, TimeUntilDisconnectNotification ttdNotification) {

        Buffer commandBuffer = buildOneWayCommandPayload();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending one-way command [{}] to [{}]",
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
                        LOG.debug("One-way command was replied with error code [{}]", errorCode);
                    } else {
                        LOG.debug("Could not send one-way command : {}.", t.getMessage());
                    }
                });
    }

    private Buffer buildOneWayCommandPayload() {
        JsonObject jsonCmd = new JsonObject().put("avg. temerature: ", getTemperatureAverage());
        return Buffer.buffer(jsonCmd.encodePrettily());
    }

    private double getTemperatureAverage() {
        return temperatureValueList.stream()
                .mapToDouble(value -> value)
                .average().orElse(0);
    }

    /**
     * Handler method for a Message from Hono that was received as telemetry data.
     * <p>
     * The tenant, the device, the payload, the content-type, the creation-time and the application properties
     * will be logged.
     *
     * @param msg The message that was received.
     */
    private void handleTelemetryMessage(DownstreamMessage<AmqpMessageContext> msg) {
        LOG.debug("received telemetry data [tenant: {}, device: {}, content-type: {}]: [{}].",
                msg.getTenantId(), msg.getDeviceId(), msg.getContentType(), msg.getPayload());
    }

    /**
     * Handler method for a Message from Hono that was received as event data.
     * <p>
     * The tenant, the device, the payload, the content-type, the creation-time and the application properties will
     * be logged.
     *
     * @param msg The message that was received.
     */
    private static void handleEventMessage(final DownstreamMessage<AmqpMessageContext> msg) {
        LOG.debug("received event [tenant: {}, device: {}, content-type: {}]: [{}].",
                msg.getTenantId(), msg.getDeviceId(), msg.getContentType(), msg.getPayload());
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public List<Double> getTemperatureValueList() {
        return temperatureValueList;
    }

    @Override
    public String toString() {
        return "AvgTemperatureNorthboundApp{" + "temperatureValueList=" + temperatureValueList +
                '}';
    }
}

