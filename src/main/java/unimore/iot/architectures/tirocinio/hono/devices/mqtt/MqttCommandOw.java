package unimore.iot.architectures.tirocinio.hono.devices.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static unimore.iot.architectures.tirocinio.hono.constants.HonoConstants.*;

/**
 * Demo class that Subscribe on the topic c///q/#
 * for receiving one-way commands
 */

public class MqttCommandOw {

    private static final Logger LOG = LoggerFactory.getLogger(MqttCommandOw.class);
    private static final String TOPIC = "c///q/#";
    private static final String MQTT_BASE_URL = String.format("tcp://%s:%d",
            HONO_HOST,
            HONO_MQTT_ADAPTER_PORT);
    private static final int QOS = 0;
    private static IMqttClient client;
    private static MqttConnectOptions options;

    public MqttCommandOw() {
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
            MqttCommandOw consumer = new MqttCommandOw();
            client = new MqttClient(MQTT_BASE_URL, clientId, new MemoryPersistence());
            consumer.connect(options, clientId);
            subscribe();

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private static void subscribe() throws MqttException {
        client.subscribe(TOPIC, QOS, (s, cmdMessage) -> {
            byte[] payload = cmdMessage.getPayload();
            LOG.info("Received from topic ({}) Command -> {}", s, new String(payload));
        });
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
