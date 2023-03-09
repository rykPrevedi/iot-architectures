package unimore.iot.architectures.tirocinio.hono.devices;

import io.vertx.core.Vertx;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.Constants.HonoConstants;

import java.util.UUID;

/**
 * MQTT Consumer using the library Eclipse Paho
 * It subscribes to a command topic and waits for generic commands.
 * When the device subscribes or unsubscribes itself the Empty Notifications will send by the MQTT protocol adapter automatically
 * <p>
 * <p>
 * mosquitto_sub -v -q 0 -h 192.168.181.17 -p 30124 -u mqttconsumer@mytenant -P mqttconsumerpassword -t command///req/#
 *
 * @author Riccardo Prevedi
 * @created 27/02/2023 - 17:02
 * @project architectures-iot
 */


public class MqttCommandConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(MqttCommandConsumer.class);
    private static final String TENANT_ID = "mytenant";
    private static final String AUTH_ID = "mqttconsumer";
    private static final String MQTT_PASSWORD = "mqttconsumerpassword";
    private static final String TOPIC = "command///req/#";
    private static final int QOS = 0;
    private static IMqttClient client;
    private static MqttConnectOptions options;

    private final Vertx vertx;
    private static final long DELAY_FOR_DISCONNECTION = 300 * 1000; // millisecond, 5 min

    public MqttCommandConsumer() {
        vertx = Vertx.vertx();
        options = new MqttConnectOptions();
        options.setUserName(AUTH_ID + "@" + TENANT_ID);
        options.setPassword(MQTT_PASSWORD.toCharArray());
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);   // seconds
    }

    public static void main(String[] args) {

        LOG.info("MQTT Command Consumer started !");
        try {
            MqttCommandConsumer commandConsumer = new MqttCommandConsumer();

            String clientId = UUID.randomUUID().toString();

            String mqttAdapterUrl = String.format("tcp://%s:%d",
                    HonoConstants.HONO_HOST,
                    HonoConstants.HONO_MQTT_ADAPTER_PORT);

            client = new MqttClient(mqttAdapterUrl, clientId, new MemoryPersistence());

            commandConsumer.connect(options, clientId);

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    /**
     * SUBSCRIBE with QoS 0 and sends the Time Until Disconnect notification with value -1
     * and RECEIVE command until UNSUBSCRIBE or Disconnect
     * UNSUBSCRIBE and sends the Time Until Disconnect notification with value 0
     */
    private void subscribe() throws MqttException {
        client.subscribe(TOPIC, QOS, (s, cmdMessage) -> {
            byte[] payload = cmdMessage.getPayload();
            LOG.info("Temperature Avg Received ({}) Command -> {}", TOPIC, new String(payload));
        });
        vertx.setTimer(DELAY_FOR_DISCONNECTION, id -> disconnect(client.getClientId()));
    }

    private void connect(MqttConnectOptions options, String clientId) throws MqttException {
        client.connect(options);
        if (client.isConnected()) {
            LOG.info("Connected to the HONO Mqtt Adapter ! ClientID: [{}]", clientId);
            LOG.info("Waiting for commands ... ");
            subscribe();
        } else {
            LOG.error("Connection could not be established");
        }
    }

    private void disconnect(String clientId) {
        try {
            client.disconnect();
            client.close();
            LOG.info("[{}] Disconnects", clientId);
            System.exit(0);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}


