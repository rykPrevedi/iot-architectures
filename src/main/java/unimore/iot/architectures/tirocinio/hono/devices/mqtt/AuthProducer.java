package unimore.iot.architectures.tirocinio.hono.devices.mqtt;

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
 * @author Riccardo Prevedi
 * @created 25/02/2023 - 17:47
 * @project architectures-iot
 */

public class AuthProducer {

    private static final Logger LOG = LoggerFactory.getLogger(AuthProducer.class);
    private static final String MQTT_USERNAME = "mydevice@mytenant";
    private static final String MQTT_PASSWORD = "mypassword";
    private static final String TOPIC = "/sensor/temperature";
    private static final int MESSAGE_COUNT = 100;
    private static IMqttClient client;
    private static MqttConnectOptions options;

    public AuthProducer() {
        options = new MqttConnectOptions();
        options.setUserName(MQTT_USERNAME);
        options.setPassword(MQTT_PASSWORD.toCharArray());
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);   // maximum seconds for the connection establishment
    }

    public static void main(String[] args) {

        LOG.info("client started ... ");

        try {

            AuthProducer producer = new AuthProducer();

            String clientId = UUID.randomUUID().toString();

            String mqttAdapterUrl = String.format("tcp://%s:%d", HonoConstants.HONO_HOST, HonoConstants.HONO_MQTT_ADAPTER_PORT);

            client = new MqttClient(mqttAdapterUrl, clientId, new MemoryPersistence());

            producer.connect(options,clientId);

            Thread.sleep(1000);

            producer.disconnect(clientId);

        } catch (MqttException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void connect(MqttConnectOptions options, String clientId) throws MqttException {
        client.connect(options);
        if (client.isConnected()) {
            LOG.info("[{}] Connects ! to the HONO mqtt adapter", clientId);
        } else
            LOG.error("connection could not be established");
    }

    private void disconnect(String clientId) throws MqttException {
        client.disconnect();
        client.close();
        LOG.info("[{}] Disconnects", clientId);
    }
}
