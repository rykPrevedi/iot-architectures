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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;


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
    private static final Boolean SEND_ONE_WAY_COMMANDS = true;
    private static final String COMMAND_SEND_LIFECYCLE_INFO = "sendLifecycleInfo";
    private static final Random RAND = new Random();
    private MessageConsumer eventConsumer;
    private MessageConsumer telemetryConsumer;
    protected final LifecycleStatus lifecycleStatus = new LifecycleStatus();


    public DemoSolution() {
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
                .onFailure(new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable t) {
                        if (t instanceof ServiceInvocationException) {
                            final int errorCode = ((ServiceInvocationException) t).getErrorCode();
                            LOG.debug("Command was replied with error code [{}].", errorCode);
                        } else {
                            LOG.debug("Could not send command : {}.", t.getMessage());
                        }
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
    private void sendOneWayCommandToAdapter(final String tenantId, final String deviceId,
                                            final TimeUntilDisconnectNotification ttdNotification) {

        final Buffer commandBuffer = buildOneWayCommandPayload();


        LOG.info("Sending one-way command [{}] to [{}].",
                COMMAND_SEND_LIFECYCLE_INFO, ttdNotification.getTenantAndDeviceId());
        client.sendOneWayCommand(tenantId, deviceId, COMMAND_SEND_LIFECYCLE_INFO, commandBuffer)
                .onSuccess(new Handler<Void>() {
                    @Override
                    public void handle(Void statusResult) {
                        LOG.info("Successfully sent one-way command payload: [{}] and received status [{}].",
                                commandBuffer.toString(), statusResult);
                    }
                })
                .onFailure(t -> {
                    if (t instanceof ServiceInvocationException) {
                        final int errorCode = ((ServiceInvocationException) t).getErrorCode();
                        LOG.error("One-way command was replied with error code [{}].", errorCode);
                    } else {
                        LOG.error("Could not send one-way command : {}.", t.getMessage());
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
        // let the command expire when the notification expires
        return Duration.ofMillis(notification.getMillisecondsUntilExpiry());
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

    /**
     * Handler method for a <em>device ready for command</em> notification (by an explicit event or contained
     * implicitly in another message).
     * <p>
     * For notifications with a positive ttd value (as usual for request-response protocols), the
     * code creates a simple command in JSON.
     * <p>
     *
     * @param notification The notification containing the tenantId, deviceId and the Instant (that
     *                     defines until when this notification is valid). See {@link TimeUntilDisconnectNotification}.
     */
    private void handleCommandReadinessNotification(TimeUntilDisconnectNotification notification) {
        LOG.info("Device is ready to receive a command : [{}].", notification);
        sendCommand(notification);
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
