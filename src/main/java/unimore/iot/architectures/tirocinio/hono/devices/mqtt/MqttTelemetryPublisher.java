package unimore.iot.architectures.tirocinio.hono.devices.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.constants.HonoConstants;
import unimore.iot.architectures.tirocinio.hono.model.EngineTemperatureSensor;

import static unimore.iot.architectures.tirocinio.hono.constants.HonoConstants.*;


import java.util.UUID;

/**
 * Demo class that publish telemetry data on the "t" topic
 * {} json string that will be sent
 *
 * @author Riccardo Prevedi
 * @created 25/02/2023 - 17:47
 * @project architectures-iot
 */

public class MqttTelemetryPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(MqttTelemetryPublisher.class);
    private static final String TOPIC = "telemetry";
    private static final String METADATA = "/?content-type=application%2Fjson";
    private static final String MQTT_BASE_URL = String.format("tcp://%s:%d",
            HONO_HOST,
            HONO_MQTT_ADAPTER_PORT);
    private static final int QOS = 0;
    private static IMqttClient client;
    private static MqttConnectOptions options;

    public MqttTelemetryPublisher() {
        options = new MqttConnectOptions();
        options.setUserName(mqttDeviceAuthId + "@" + MY_TENANT_ID);
        options.setPassword(devicePassword.toCharArray());
        options.setAutomaticReconnect(true);
        options.setConnectionTimeout(10);       // maximum seconds for the connection establishment
    }

    public static void main(String[] args) {
        LOG.info("Client MQTT started ... ");
        final String payloadString = "{\"temp\": 5}";
        try {
            String clientId = UUID.randomUUID().toString();
            MqttTelemetryPublisher producer = new MqttTelemetryPublisher();
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
            LOG.debug("(If Authorized by MQTT adapter) Data Correctly Published !");
        } else {
            LOG.error("Error: Topic or Msg = Null or MQTT Client is not Connected !");
        }
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
