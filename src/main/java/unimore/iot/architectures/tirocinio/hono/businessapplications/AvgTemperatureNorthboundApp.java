package unimore.iot.architectures.tirocinio.hono.businessapplications;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.eclipse.hono.application.client.DownstreamMessage;
import org.eclipse.hono.application.client.MessageConsumer;
import org.eclipse.hono.application.client.amqp.AmqpApplicationClient;
import org.eclipse.hono.application.client.amqp.AmqpMessageContext;
import org.eclipse.hono.application.client.amqp.ProtonBasedApplicationClient;
import org.eclipse.hono.client.amqp.config.ClientConfigProperties;
import org.eclipse.hono.client.amqp.connection.HonoConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.Constants.HonoConstants;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;


/**
 * @author Riccardo Prevedi
 * @created 05/03/2023 - 18:45
 * @project architectures-iot
 */

public class AvgTemperatureNorthboundApp {
    private static final Logger LOG = LoggerFactory.getLogger(AvgTemperatureNorthboundApp.class);
    private static final String HONO_CLIENT_USER = "consumer@HONO";
    private static final String HONO_CLIENT_PASSWORD = "verysecret";
    private static final int RECONNECT_ATTEMPTS = 2;

    private final AmqpApplicationClient client;
    private final Vertx vertx = Vertx.vertx();  // embedded Vertx instance

    private MessageConsumer telemetryConsumer;
    private MessageConsumer eventConsumer;
    //private static final String COMMAND_SEND_TEMPERATURE = "temperature";

    private String tenant;

    public AvgTemperatureNorthboundApp() {
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
            LOG.info("Consumer ready for telemetry and event messages");
            LOG.info("Tenant: {}", getTenant());
        } catch (CompletionException e) {
            LOG.error("{} consumer failed to start [{}:{}]",
                    "AMQP", HonoConstants.HONO_HOST, HonoConstants.HONO_AMQP_CONSUMER_PORT, e.getCause());
        }
    }

    private Future<MessageConsumer> createEventConsumer() {
        return client.createEventConsumer(
                        tenant,
                        msg -> {
                            // handle command readiness notification
                            //msg.getTimeUntilDisconnectNotification().ifPresent(this::handleCommandReadinessNotification);
                            //handleEventMessage(msg);
                        },
                        cause -> LOG.error("event consumer closed by remote", cause))
                .onSuccess(consumer -> this.eventConsumer = consumer);
    }

    private Future<MessageConsumer> createTelemetryConsumer() {
        return client.createTelemetryConsumer(
                        tenant,
                        msg -> {
                            // handle command readiness notification
                            //msg.getTimeUntilDisconnectNotification().ifPresent(this::handleCommandReadinessNotification);
                            handleTelemetryMessage(msg);
                        },
                        cause -> LOG.error("telemetry consumer closed by remote", cause))
                .onSuccess(consumer -> this.telemetryConsumer = consumer);
    }

    private void handleTelemetryMessage(DownstreamMessage<AmqpMessageContext> downstreamMessage) {
        LOG.info("received telemetry data [tenant: {}, device: {}, content-type: {}]: [{}].",
                downstreamMessage.getTenantId(),
                downstreamMessage.getDeviceId(),
                downstreamMessage.getContentType(),
                downstreamMessage.getPayload());
    }


    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }
}

