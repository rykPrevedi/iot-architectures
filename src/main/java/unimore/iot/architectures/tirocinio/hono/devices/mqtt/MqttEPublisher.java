package unimore.iot.architectures.tirocinio.hono.devices.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttPubAck;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttPublish;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttWireMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static unimore.iot.architectures.tirocinio.hono.constants.HonoConstants.*;

/**
 * Demo class that Publish events on the "e" topic
 * Publishing {"alarm": 1} json string
 */

public class MqttEPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(MqttEPublisher.class);
    private static final String TOPIC = "e";
    private static final String METADATA = "/?content-type=application%2Fjson";
    private static final String MQTT_BASE_URL = String.format("tcp://%s:%d",
            HONO_HOST,
            HONO_MQTT_ADAPTER_PORT);
    private static final int QOS = 1;
    private static IMqttClient client;
    private static MqttConnectOptions options;

    public MqttEPublisher() {
        options = new MqttConnectOptions();
        options.setUserName(mqttDeviceAuthId + "@" + MY_TENANT_ID);
        options.setPassword(devicePassword.toCharArray());
        options.setAutomaticReconnect(true);
        options.setConnectionTimeout(10);
    }

    public static void main(String[] args) {
        LOG.info("Client MQTT started ... ");
        final String payloadString = "{\"alarm\": 1}";
        try {
            String clientId = UUID.randomUUID().toString();
            MqttEPublisher producer = new MqttEPublisher();
            client = new MqttClient(MQTT_BASE_URL, clientId, new MemoryPersistence());
            producer.connect(options, clientId);
            publishData(payloadString);
            disconnect(clientId);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private static void publishData(String msgString) throws MqttException {
        LOG.info("Publishing to Topic: {} Data: {}", TOPIC, msgString);

        if (msgString != null) {
            MqttMessage message = new MqttMessage(msgString.getBytes());
            message.setQos(QOS);
            message.setRetained(false);
            client.publish(TOPIC + METADATA, message);
            LOG.debug("Data Correctly Published !");
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
