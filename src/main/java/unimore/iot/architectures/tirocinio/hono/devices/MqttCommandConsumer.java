package unimore.iot.architectures.tirocinio.hono.devices;

import io.vertx.core.json.JsonObject;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.Constants.HonoConstants;

import java.util.UUID;


/**
 * MQTT Consumer using the library Eclipse Paho
 * Subscribing and wait for commands and consuming JSON messages.
 *
 * mosquitto_sub -v -q 0 -h 192.168.181.17 -p 30124 -u mqttconsumer@mytenant -P mqttconsumerpassword -t command///req/#
 *
 * @author Riccardo Prevedi
 * @created 27/02/2023 - 17:02
 * @project architectures-iot
 */

public class MqttCommandConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(MqttCommandConsumer.class);
    private static final String MQTT_USERNAME = "mqttconsumer@mytenant";
    private static final String MQTT_PASSWORD = "mqttconsumerpassword";
    private static final String TOPIC = "command///req/#";
    private static final int QoS = 0;
    private static IMqttClient client;
    private static MqttConnectOptions options;

    public MqttCommandConsumer() {
        options = new MqttConnectOptions();
        options.setUserName(MQTT_USERNAME);
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

            subscribe();

            Thread.sleep(50 * 1000);

            commandConsumer.disconnect(clientId);

        } catch (MqttException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * SUBSCRIBE to TOPIC command///req/#
     * with semantic AT LEAST ONCE (QoS 0)
     * Receive command until UNSUBSCRIBE or Disconnect
     *
     * //TODO: implement unsubscribtion
     */
    private static void subscribe() throws MqttException {
        client.subscribe(TOPIC, QoS, new IMqttMessageListener() {
            @Override
            public void messageArrived(String s, MqttMessage cmdMessage) {
                byte[] payload = cmdMessage.getPayload();
                LOG.info("JsonObject Received ({}) Command -> {}", TOPIC, new String(payload));
            }
        });
    }

    private void connect(MqttConnectOptions options, String clientId) throws MqttException {
        client.connect(options);
        if(client.isConnected()){
            LOG.info("Connected to the HONO Mqtt Adapter ! ClientID: [{}]", clientId);
            LOG.info("Waiting for commands ... ");
        } else {
            LOG.error("Connection could not be established");
        }
    }

    private void disconnect(String clientId) throws MqttException {
        client.disconnect();
        client.close();
        LOG.info("[{}] Disconnects", clientId);
    }
}

