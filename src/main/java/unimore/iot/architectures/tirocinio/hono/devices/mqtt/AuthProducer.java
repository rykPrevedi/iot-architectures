package unimore.iot.architectures.tirocinio.hono.devices.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.Constants.HonoConstants;
import unimore.iot.architectures.tirocinio.hono.devices.mqtt.model.EngineTemperatureSensor;

import java.util.UUID;

/**
 *
 * mosquitto_pub -h 192.168.181.17 -p 30124 -u mydevice@mytenant -P mypassword -t telemetry -m '{"temp": 3}'
 *
 * @author Riccardo Prevedi
 * @created 25/02/2023 - 17:47
 * @project architectures-iot
 */

public class AuthProducer {

    private static final Logger LOG = LoggerFactory.getLogger(AuthProducer.class);
    private static final String TENANT_ID = "mytenant";
    private static final String AUTH_ID = "device-mqtt";
    private static final String PASSWORD = "hono-secret";
    private static final String TOPIC = "telemetry";    // telemetry/mytenant/mydevice
    private static final String METADATA = "/?content-type=text%2Fplain";
    private static final int QOS = 0;
    private static final int MESSAGE_COUNT = 100;
    private static IMqttClient client;
    private static MqttConnectOptions options;

    public AuthProducer() {
        options = new MqttConnectOptions();
        options.setUserName(AUTH_ID + "@" + TENANT_ID);
        options.setPassword(PASSWORD.toCharArray());
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);   // maximum seconds for the connection establishment
    }

    public static void main(String[] args) {

        LOG.info("Client Auth started ... ");

        try {

            AuthProducer producer = new AuthProducer();

            String clientId = UUID.randomUUID().toString();

            String mqttAdapterUrl = String.format("tcp://%s:%d", HonoConstants.HONO_HOST, HonoConstants.HONO_MQTT_ADAPTER_PORT);

            client = new MqttClient(mqttAdapterUrl, clientId, new MemoryPersistence());

            producer.connect(options, clientId);

            EngineTemperatureSensor engineTemperatureSensor = new EngineTemperatureSensor();

            for (int i = 0; i < MESSAGE_COUNT; i++) {

                double sensorValue = engineTemperatureSensor.getTemperatureValue();
                String payloadString = Double.toString(sensorValue);

                publishData(payloadString);

                Thread.sleep(1000);
            }

            disconnect(clientId);

        } catch (MqttException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Send a target String Payload to the specified MQTT topic.
     *
     * The MQTT adapter exposes a names/filters topic hierarchy based on the functionality of Hono.
     * Only QoS 0 and QoS 1 are supported
     * devices can add arbitrary metadata, but only content-type property is supported
     *
     * @throws MqttException
     */
    private static void publishData(String msgString) throws MqttException {
        LOG.info("Publishing to Topic: {} Data: {}", TOPIC, msgString);

        if (msgString != null) {
            MqttMessage message = new MqttMessage(msgString.getBytes());
            message.setQos(QOS);
            message.setRetained(false); // Hono does not support Retaining messages
            client.publish(TOPIC + METADATA, message);

            LOG.debug("(If Authorized by MQTT adapter) Data Correctly Published !");
        } else {
            LOG.error("Error: Topic or Msg = Null or MQTT Client is not Connected !");
        }
    }

    private void connect(MqttConnectOptions options, String clientId) throws MqttException {
        client.connect(options);
        if (client.isConnected()) {
            LOG.info("Connected to the HONO Mqtt Adapter ! ClientID: [{}]", clientId);
        } else
            LOG.error("connection could not be established");
    }

    private static void disconnect(String clientId) throws MqttException {
        client.disconnect();
        client.close();
        LOG.info("[{}] Disconnects", clientId);
    }
}
