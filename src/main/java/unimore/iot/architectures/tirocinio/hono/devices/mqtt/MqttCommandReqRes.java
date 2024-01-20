package unimore.iot.architectures.tirocinio.hono.devices.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.regex.Pattern;

import static unimore.iot.architectures.tirocinio.hono.constants.HonoConstants.*;

/**
 * Demo class incomplete, for solution visit:
 * {@link <a href="https://stackoverflow.com/questions/31161740/how-to-publish-a-message-while-receiving-on-a-java-mqtt-client-using-eclipse-pah">...</a>}
 */

public class MqttCommandReqRes {

    private static final Logger LOG = LoggerFactory.getLogger(MqttCommandReqRes.class);
    private static final String TOPIC_REQ = "c///q/#";
    private static final String MQTT_BASE_URL = String.format("tcp://%s:%d",
            HONO_HOST,
            HONO_MQTT_ADAPTER_PORT);
    private static final int QOS = 0;
    private static IMqttClient client;
    private static MqttConnectOptions options;
    public static String commandReqId;
    private static final String METADATA = "/?content-type=application%2Fjson";
    private static final Integer COMMAND_STATUS_EXE_CODE = 200;
    private static final String BRIGHTNESS_CHANGED_TRUE = "{\"brightness-changed\": true}";

    public MqttCommandReqRes() {
        options = new MqttConnectOptions();
        options.setUserName(mqttDeviceAuthId + "@" + MY_TENANT_ID);
        options.setPassword(devicePassword.toCharArray());
        options.setAutomaticReconnect(true);
        options.setConnectionTimeout(10);
    }

    public static void main(String[] args) {
        LOG.info("Client MQTT started ... ");
        try {
            String clientId = UUID.randomUUID().toString();
            MqttCommandReqRes consumer = new MqttCommandReqRes();
            client = new MqttClient(MQTT_BASE_URL, clientId, new MemoryPersistence());
            consumer.connect(options, clientId);
            subscribe();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private static void subscribe() throws MqttException {
        client.subscribe(TOPIC_REQ, QOS, (s, cmdMessage) -> {
            byte[] payload = cmdMessage.getPayload();
            LOG.info("Received from topic ({}) Command -> {}", s, new String(payload));
            String[] output = s.split(Pattern.quote("/"));
            commandReqId = output[4];
        });
    }

    private static void publishData() throws MqttException {
        //c///s/${req-id}/${status}
        final String topicRes = String.format("c///s/%s/%d", commandReqId, COMMAND_STATUS_EXE_CODE);
        LOG.info("Publishing to Topic: {} Data: {}", topicRes, BRIGHTNESS_CHANGED_TRUE);
        MqttMessage message = new MqttMessage(BRIGHTNESS_CHANGED_TRUE.getBytes());
        message.setQos(QOS);
        message.setRetained(false);
        client.publish(topicRes + METADATA, message);
        LOG.debug("Data Correctly Published !");
    }

    private void connect(MqttConnectOptions options, String clientId) throws MqttException {
        IMqttToken iMqttToken = client.connectWithResult(options);
        if (client.isConnected()) {
            LOG.info("Connected to the HONO Mqtt Adapter ! ClientID: [{}]", clientId);
            LOG.info("Context : {}", iMqttToken.getUserContext());
        } else
            LOG.error("connection could not be established");
    }

    private static void disconnect(String clientId) throws MqttException {
        client.disconnect();
        client.close();
        LOG.info("[{}] Disconnects", clientId);
    }
}
