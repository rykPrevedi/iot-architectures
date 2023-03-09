package unimore.iot.architectures.tirocinio.hono.devices;

import io.vertx.core.Vertx;
import org.apache.qpid.proton.message.Message;
import org.eclipse.hono.client.amqp.config.ClientConfigProperties;
import org.eclipse.hono.client.amqp.connection.AmqpUtils;
import org.eclipse.hono.client.amqp.connection.HonoConnection;
import org.eclipse.hono.client.device.amqp.AmqpAdapterClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.Constants.HonoConstants;

/**
 * @author Riccardo Prevedi
 * @created 08/03/2023 - 15:25
 * @project architectures-iot
 */

public class AmqpCommandConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(AmqpCommandConsumer.class);
    private static final String TENANT_ID = "mytenant";
    private static final String AUTH_ID = "device-amqp";
    private static final String PASSWORD = "hono-secret";
    private static final int RECONNECT_ATTEMPTS = 5;

    // A Vert.x based client for interacting with Hono's AMPQ adapter
    private AmqpAdapterClient adapterClient;

    private final ClientConfigProperties config = new ClientConfigProperties();
    private final Vertx vertx = Vertx.vertx();  // embedded Vertx instance

    public AmqpCommandConsumer() {
        config.setHost(HonoConstants.HONO_HOST);
        config.setPort(HonoConstants.HONO_AMQP_ADAPTER_PORT);
        config.setUsername(AUTH_ID + "@" + TENANT_ID);
        config.setPassword(PASSWORD);
        config.setReconnectAttempts(RECONNECT_ATTEMPTS);
    }

    public static void main(String[] args) {
        AmqpCommandConsumer amqpCommandConsumer = new AmqpCommandConsumer();
        amqpCommandConsumer.connect();
    }

    private void connect() {
        HonoConnection connection = HonoConnection.newConnection(vertx, config);
        connection.connect()
                .onSuccess(connection1 -> {
                    LOG.info("connected to the AMPQ Adapter [{}]", connection1.getRemoteContainerId());
                    startDevice(connection1);
                })
                .onFailure(t -> {
                    LOG.error("Failed to establish connection: ", t);
                    System.exit(1);
                });
    }

    private void startDevice(HonoConnection connection) {
        adapterClient = AmqpAdapterClient.create(connection);

        LOG.info("Waiting for commands...");
        adapterClient.createCommandConsumer(this::handleCommand);
    }


    private void handleCommand(Message commandMessage) {
        String subject = commandMessage.getSubject();
        String commandPayload = AmqpUtils.getPayloadAsString(commandMessage);

        // one-way command
        LOG.info("Received one-way command [name : {}]: {}", subject, commandPayload);
    }
}
