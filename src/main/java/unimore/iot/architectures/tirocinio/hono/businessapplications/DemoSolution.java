package unimore.iot.architectures.tirocinio.hono.businessapplications;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.eclipse.hono.application.client.*;
import org.eclipse.hono.application.client.amqp.AmqpApplicationClient;
import org.eclipse.hono.application.client.amqp.ProtonBasedApplicationClient;
import org.eclipse.hono.client.ServiceInvocationException;
import org.eclipse.hono.client.amqp.config.ClientConfigProperties;
import org.eclipse.hono.client.amqp.connection.HonoConnection;
import org.eclipse.hono.util.Lifecycle;
import org.eclipse.hono.util.LifecycleStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.constants.HonoConstants;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static unimore.iot.architectures.tirocinio.hono.constants.HonoConstants.*;


/**
 * The devices connected to Hono belonging to the tenant can send an empty message
 * indicating the number of seconds they will wait for a response by setting a header or a query parameter and
 * a command will be sent to them.
 * <p>
 * The code consumes data until it receives any input on its console (which finishes it and closes vertx).
 */
public class DemoSolution {

    private static final Logger LOG = LoggerFactory.getLogger(DemoSolution.class);
    private static final String HONO_CLIENT_USER = "consumer@HONO";
    private static final String HONO_CLIENT_PASSWORD = "verysecret";
    private final Vertx vertx = Vertx.vertx();  // embedded Vertx instance
    private final ApplicationClient<? extends MessageContext> client;
    private Boolean SEND_ONE_WAY_COMMANDS = false;
    private static final String COMMAND_SEND_LIFECYCLE_INFO = "sendLifecycleInfo";
    private static final Random RAND = new Random();
    private MessageConsumer eventConsumer;
    private MessageConsumer telemetryConsumer;
    protected final LifecycleStatus lifecycleStatus = new LifecycleStatus();

    private final Map<String, TimeUntilDisconnectNotification> pendingTtdNotification = new HashMap<>();

    private final Map<String, Handler<Void>> periodicCommandSenderTimerCancelerMap = new HashMap<>();


    public DemoSolution() {
        this.client = createAmqpApplicationClient();
    }

    /**
     * Class constructor
     *
     * @param isOwCommand default false
     */
    public DemoSolution(Boolean isOwCommand) {
        this.SEND_ONE_WAY_COMMANDS = isOwCommand;
        this.client = createAmqpApplicationClient();
    }

    /**
     * The consumer needs one connection to the AMQP 1.0 messaging network from which it can consume data.
     * The client for receiving data is instantiated here.
     */
    private ApplicationClient<? extends MessageContext> createAmqpApplicationClient() {
        ClientConfigProperties props = new ClientConfigProperties();
        props.setLinkEstablishmentTimeout(5000L);                // 5 sec
        props.setHost(HonoConstants.HONO_HOST);                  // k8s-node-1's IP
        props.setPort(HonoConstants.HONO_AMQP_CONSUMER_PORT);    // qdrouter service port
        props.setUsername(HONO_CLIENT_USER);
        props.setPassword(HONO_CLIENT_PASSWORD);                // default passwords of the Hono Sandbox installation
        // for ease of use.
        return new ProtonBasedApplicationClient(HonoConnection.newConnection(vertx, props));
    }

    /**
     * Start the application client and set the message handling method to treat data that is received.
     */
    public void consumeData() {

        // instantiate the CompletableFuture, the asynchronous Java Object
        CompletableFuture<ApplicationClient<? extends MessageContext>> startup = new CompletableFuture<>();

        final AmqpApplicationClient ac = (AmqpApplicationClient) client;
        ac.addDisconnectListener(honoConnection -> LOG.info("lost connection to Hono, trying to reconnect ..."));
        ac.addReconnectListener(honoConnection -> LOG.info("reconnected to Hono"));

        final Promise<Void> readyTacker = Promise.promise();
        client.addOnClientReadyHandler(readyTacker);
        client
                .start()
                .compose(ok -> readyTacker.future())
                .compose(v -> Future.all(createTelemetryConsumer(), createEventConsumer()))
                .onSuccess(ok -> startup.complete(client))
                .onFailure(startup::completeExceptionally);

        try {
            startup.join();
            LOG.info("Consumer ready for telemetry and event messages");
            System.in.read();
        } catch (final CompletionException e) {
            LOG.error("{} consumer failed to start [{}:{}]",
                    "AMQP", HonoConstants.HONO_HOST, HonoConstants.HONO_AMQP_CONSUMER_PORT, e.getCause());
        } catch (final IOException e) {
            // nothing we can do
        }


        final CompletableFuture<ApplicationClient<? extends MessageContext>> shutdown = new CompletableFuture<>();

        final List<Future<Void>> closeFutures = new ArrayList<>();
        Optional.ofNullable(eventConsumer)
                .map(MessageConsumer::close)
                .ifPresent(closeFutures::add);
        Optional.ofNullable(telemetryConsumer)
                .map(MessageConsumer::close)
                .ifPresent(closeFutures::add);
        Optional.of(client)
                .map(Lifecycle::stop)
                .ifPresent(closeFutures::add);

        Future.join(closeFutures)
                .compose(ok -> vertx.close())
                .recover(throwable -> vertx.close())
                .onComplete(new Handler<AsyncResult<Void>>() {
                    @Override
                    public void handle(AsyncResult<Void> voidAsyncResult) {
                        shutdown.complete(client);
                    }
                });

        // wait for clients to be closed
        shutdown.join();
        LOG.info("Consumer has been shut down");
    }

    /**
     * The app tries to connect to the specific AMQP 1.0 Endpoint event/TENANT
     * Create the message consumer that handles event messages and invokes the notification callback
     * {@link #handleCommandReadinessNotification(TimeUntilDisconnectNotification)} if the message indicates that it
     * stays connected for a specified time.
     *
     * @return A succeeded future if the creation was successful, a failed Future otherwise.
     */
    private Future<MessageConsumer> createEventConsumer() {
        return client.createEventConsumer(
                        HonoConstants.MY_TENANT_ID,
                        msg -> {
                            // handle command readiness notification
                            msg.getTimeUntilDisconnectNotification().ifPresent(this::handleCommandReadinessNotification);
                            handleEventMessage(msg);
                        },
                        cause -> LOG.error("event consumer closed by remote", cause))
                .onSuccess(consumer -> this.eventConsumer = consumer);
    }


    /**
     * Create the message consumer that handles telemetry messages and invokes the notification callback
     * {@link #handleCommandReadinessNotification(TimeUntilDisconnectNotification)} if the message indicates that it
     * stays connected for a specified time.
     *
     * @return A succeeded future if the creation was successful, a failed Future otherwise.
     */
    private Future<MessageConsumer> createTelemetryConsumer() {
        return client.createTelemetryConsumer(
                        HonoConstants.MY_TENANT_ID,
                        msg -> {
                            // handle command readiness notification
                            msg.getTimeUntilDisconnectNotification().ifPresent(this::handleCommandReadinessNotification);
                            handleTelemetryMessage(msg);
                        },
                        cause -> LOG.error("telemetry consumer closed by remote", cause))
                .onSuccess(consumer -> this.telemetryConsumer = consumer);
    }


    /**
     * Send a command to the device for which a {@link TimeUntilDisconnectNotification} was received.
     * <p>
     * If the contained <em>ttd</em> is set to a value @gt; 0, the commandClient will be closed after a response
     * was received.
     * If the contained <em>ttd</em> is set to -1, the commandClient will remain open for further commands to be sent.
     *
     * @param ttdNotification The ttd notification that was received for the device.
     */
    private void sendCommandToAdapter(
            final String tenantId,
            final String deviceId,
            final TimeUntilDisconnectNotification ttdNotification) {

        final Duration commandTimeout = calculateCommandTimeout(ttdNotification);
        final Buffer commandBuffer = buildCommandPayload();
        final String command = "setBrightness";
        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending command [{}] to [{}].", command, ttdNotification.getTenantAndDeviceId());
        }
        client.sendCommand(tenantId, deviceId, command, commandBuffer, "application/json", null, commandTimeout, null)
                .onSuccess(result -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Successfully sent command payload: [{}].", commandBuffer.toString());
                        LOG.debug("And received response: [{}].", Optional.ofNullable(result.getPayload())
                                .orElseGet(Buffer::buffer).toString());
                    }
                })
                .onFailure(t -> {
                    if (t instanceof ServiceInvocationException) {
                        final int errorCode = ((ServiceInvocationException) t).getErrorCode();
                        LOG.debug("Command was replied with error code [{}].", errorCode);
                    } else {
                        LOG.debug("Could not send command : {}.", t.getMessage());
                    }
                });
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
    private void sendOneWayCommandToAdapter(
            final String tenantId,
            final String deviceId,
            final TimeUntilDisconnectNotification ttdNotification) {

        final Buffer commandBuffer = buildOneWayCommandPayload();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending one-way command [{}] to [{}].",
                    COMMAND_SEND_LIFECYCLE_INFO, ttdNotification.getTenantAndDeviceId());
        }
        client.sendOneWayCommand(tenantId, deviceId, COMMAND_SEND_LIFECYCLE_INFO, commandBuffer)
                .onSuccess(statusResult -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Successfully sent one-way command payload: [{}] and received status [{}].",
                                commandBuffer.toString(), statusResult);
                    }
                })
                .onFailure(t -> {
                    if (t instanceof ServiceInvocationException) {
                        final int errorCode = ((ServiceInvocationException) t).getErrorCode();
                        LOG.debug("One-way command was replied with error code [{}].", errorCode);
                    } else {
                        LOG.debug("Could not send one-way command : {}.", t.getMessage());
                    }
                });
    }

    private static Buffer buildCommandPayload() {
        final JsonObject jsonCmd = new JsonObject().put("brightness", RAND.nextInt(100));
        return Buffer.buffer(jsonCmd.encodePrettily());
    }

    private static Buffer buildOneWayCommandPayload() {
        final JsonObject jsonCmd = new JsonObject().put("info", "app restarted.");
        return Buffer.buffer(jsonCmd.encodePrettily());
    }

    /**
     * Calculate the timeout for a command that is tried to be sent to a device for which a
     * {@link TimeUntilDisconnectNotification} was received.
     *
     * @param notification The notification that was received for the device.
     * @return The timeout (milliseconds) to be set for the command.
     */
    private Duration calculateCommandTimeout(final TimeUntilDisconnectNotification notification) {
        if (notification.getTtd() == -1) {
            // let the command expire directly before the next periodic timer is started
            return Duration.ofMillis(COMMAND_INTERVAL_FOR_DEVICES_CONNECTED_WITH_UNLIMITED_EXPIRY * 1000L);
        } else {
            // let the command expire when the notification expires
            return Duration.ofMillis(notification.getMillisecondsUntilExpiry());
        }
    }

    private void setPeriodicCommandSenderTimerCanceler(final Long timerId,
                                                       final TimeUntilDisconnectNotification ttdNotification) {
        this.periodicCommandSenderTimerCancelerMap.put(ttdNotification.getTenantAndDeviceId(), v -> {
            vertx.cancelTimer(timerId);
            periodicCommandSenderTimerCancelerMap.remove(ttdNotification.getTenantAndDeviceId());
        });
    }

    private void cancelPeriodicCommandSender(final TimeUntilDisconnectNotification notification) {
        if (isPeriodicCommandSenderActiveForDevice(notification)) {
            LOG.debug("Cancelling periodic sender for {}", notification.getTenantAndDeviceId());
            periodicCommandSenderTimerCancelerMap.get(notification.getTenantAndDeviceId()).handle(null);
        } else {
            LOG.debug("Wanted to cancel periodic sender for {}, but could not find one",
                    notification.getTenantAndDeviceId());
        }
    }

    private boolean isPeriodicCommandSenderActiveForDevice(final TimeUntilDisconnectNotification notification) {
        return periodicCommandSenderTimerCancelerMap.containsKey(notification.getTenantAndDeviceId());
    }

    /**
     * Sends a command to the device for which a {@link TimeUntilDisconnectNotification} was received.
     *
     * @param notification The notification that was received for the device.
     */
    private void sendCommand(final TimeUntilDisconnectNotification notification) {
        if (SEND_ONE_WAY_COMMANDS) {
            sendOneWayCommandToAdapter(notification.getTenantId(), notification.getDeviceId(), notification);
        } else {
            sendCommandToAdapter(notification.getTenantId(), notification.getDeviceId(), notification);
        }
    }

    private void handleCommandReadinessNotification(TimeUntilDisconnectNotification notification) {
        if (notification.getTtd() <= 0) {
            handlePermanentlyConnectedCommandReadinessNotification(notification);
        } else {
            LOG.info("Device is ready to receive a command : [{}].", notification);
            sendCommand(notification);
        }
    }

    /**
     * Handle a ttd notification for permanently connected devices.
     * <p>
     * Instead of immediately handling the notification, it is first put to a map and a timer is started to handle it
     * later. Notifications for the same device that are received before the timer expired, will overwrite the original
     * notification. By this an <em>event flickering</em> (like it could occur when starting the app while several
     * notifications were persisted in the messaging network) is handled correctly.
     * <p>
     * If the contained <em>ttd</em> is set to -1, a command will be sent periodically every
     * {@link HonoConstants#COMMAND_INTERVAL_FOR_DEVICES_CONNECTED_WITH_UNLIMITED_EXPIRY} seconds to the device
     * until a new notification was received with a <em>ttd</em> set to 0.
     *
     * @param notification The notification of a permanently connected device to handle.
     */
    private void handlePermanentlyConnectedCommandReadinessNotification(
            final TimeUntilDisconnectNotification notification) {

        final String keyForDevice = notification.getTenantAndDeviceId();

        final TimeUntilDisconnectNotification previousNotification = pendingTtdNotification.get(keyForDevice);
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
                LOG.debug("Handle device notification for [{}].", notification.getTenantAndDeviceId());
                // now take the notification from the pending map and handle it
                final TimeUntilDisconnectNotification notificationToHandle = pendingTtdNotification.remove(keyForDevice);
                if (notificationToHandle != null) {
                    if (notificationToHandle.getTtd() == -1) {
                        LOG.info("Device notified as being ready to receive a command until further notice : [{}].",
                                notificationToHandle);

                        // cancel a still existing timer for this device (if found)
                        cancelPeriodicCommandSender(notification);
                        // immediately send the first command
                        sendCommand(notificationToHandle);

                        // for devices that stay connected, start a periodic timer now that repeatedly sends a command
                        // to the device
                        vertx.setPeriodic(
                                (long) COMMAND_INTERVAL_FOR_DEVICES_CONNECTED_WITH_UNLIMITED_EXPIRY
                                        * 1000,
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
     * Handler method for a Message from Hono that was received as telemetry data.
     * <p>
     * The tenant, the device, the payload, the content-type, the creation-time and the application properties
     * will be logged.
     *
     * @param msg The message that was received.
     */
    private static void handleTelemetryMessage(final DownstreamMessage<? extends MessageContext> msg) {
        LOG.info("received telemetry data [tenant: {}, device: {}, content-type: {}]: [{}].",
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
    private static void handleEventMessage(final DownstreamMessage<? extends MessageContext> msg) {
        LOG.info("received event [tenant: {}, device: {}, content-type: {}]: [{}].",
                msg.getTenantId(), msg.getDeviceId(), msg.getContentType(), msg.getPayload());
    }


}
