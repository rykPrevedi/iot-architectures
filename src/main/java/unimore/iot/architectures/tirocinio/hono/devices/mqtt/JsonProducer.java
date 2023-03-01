package unimore.iot.architectures.tirocinio.hono.devices.mqtt;

import com.google.gson.Gson;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.Constants.HonoConstants;
import unimore.iot.architectures.tirocinio.hono.devices.mqtt.model.EngineTemperatureSensor;
import unimore.iot.architectures.tirocinio.hono.devices.mqtt.model.MessageDescriptor;

import java.util.UUID;


/**
 *
 * mosquitto_pub -h 192.168.181.17 -p 30124 -u mydevice@mytenant -P mypassword -t telemetry -m '{\"temp\": 3}'
 *
 * @author Riccardo Prevedi
 * @created 27/02/2023 - 13:55
 * @project architectures-iot
 */

public class JsonProducer {
    private static final Logger LOG = LoggerFactory.getLogger(JsonProducer.class);
    private static final String MQTT_USERNAME = "mqttjson@mytenant";
    private static final String MQTT_PASSWORD = "mqttjsonpassword";
    private static final String TOPIC = "telemetry";
    private static final String METADATA = "/?content-type=application%2Fjson";
    private static final int QOS = 0;
    private static final int MESSAGE_COUNT = 10;
    private static IMqttClient client;
    private static MqttConnectOptions options;

    public JsonProducer() {
        options = new MqttConnectOptions();
        options.setUserName(MQTT_USERNAME);
        options.setPassword(MQTT_PASSWORD.toCharArray());
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);   // maximum seconds for the connection establishment
    }

    public static void main(String[] args) {

        LOG.info("Client Json started ... ");
        try {
            JsonProducer jsonProducer = new JsonProducer();

            String clientId = UUID.randomUUID().toString();

            String mqttAdapterUrl = String.format("tcp://%s:%d", HonoConstants.HONO_HOST, HonoConstants.HONO_MQTT_ADAPTER_PORT);

            client = new MqttClient(mqttAdapterUrl, clientId, new MemoryPersistence());

            jsonProducer.connect(options, clientId);

            EngineTemperatureSensor engineTemperatureSensor = new EngineTemperatureSensor();

            for (int i = 0; i < MESSAGE_COUNT; i++) {

                double sensorValue = engineTemperatureSensor.getTemperatureValue();

                String jsonString = buildJsonMessage(sensorValue);

                publishData(jsonString);

                Thread.sleep(1000);
            }

            jsonProducer.disconnect(clientId);

        } catch (MqttException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create structure JSON message starting from the passed sensorValue
     * and using the MessageDescriptor class
     *
     * @param sensorValue the sensorValue
     * @return the JsonString
     */
    private static String buildJsonMessage(double sensorValue) {

        try {

            Gson gson = new Gson();

            MessageDescriptor messageDescriptor = new MessageDescriptor(MessageDescriptor.ENGINE_TEMPERATURE_SENSOR, sensorValue);

            return gson.toJson(messageDescriptor);

        } catch (Exception e) {
            LOG.error("Error creating json payload ! Message: {}", e.getLocalizedMessage());
            return null;
        }
    }

    /**
     * Send a target String Payload to the specified MQTT topic.
     * <p>
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
            LOG.info("[{}] Connects ! to the HONO Mqtt Adapter", clientId);
        } else
            LOG.error("connection could not be established");
    }

    private void disconnect(String clientId) throws MqttException {
        client.disconnect();
        client.close();
        LOG.info("[{}] Disconnects", clientId);
    }
}
