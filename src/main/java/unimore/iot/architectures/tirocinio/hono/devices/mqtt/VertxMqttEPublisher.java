package unimore.iot.architectures.tirocinio.hono.devices.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

import static unimore.iot.architectures.tirocinio.hono.constants.HonoConstants.*;


public class VertxMqttEPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(VertxMqttEPublisher.class);
    private static final String clientId = UUID.randomUUID().toString();
    private static final String EVENT_TOPIC = "e";
    private static final Vertx vertx = Vertx.vertx();
    private static final String METADATA = "/?content-type=application%2Fjson";
    private static MqttClient client;
    private static  Integer counter = 1;
    private static Long timerId;

    public VertxMqttEPublisher() {
        MqttClientOptions options = new MqttClientOptions()
                .setUsername(mqttDeviceAuthId + "@" + MY_TENANT_ID)
                .setPassword(devicePassword)
                .setClientId(clientId);
        client = MqttClient.create(vertx, options);
    }

    public static void main(String[] args) {
        VertxMqttEPublisher ePublisher = new VertxMqttEPublisher();
        ePublisher.startDevice();
    }
    private static void publishData(String payloadString) {
        client.publish(
                EVENT_TOPIC + METADATA,
                Buffer.buffer(payloadString),
                MqttQoS.AT_LEAST_ONCE,
                false,
                false,
                s -> LOG.info("Publish sent to the Adapter"));
    }
    private void startDevice() {
        client.publishCompletionHandler(p -> {
            LOG.info("Received PUBACK: {}", p);
            if (counter == 11){
                vertx.cancelTimer(timerId);
                client.disconnect()
                        .onSuccess(d -> LOG.info("Client disconnected"))
                        .onFailure(t ->
                                LOG.error("Problems disconnecting the client",
                                t.getCause()));
                vertx.close()
                        .onSuccess(ok -> LOG.info("Vertx instance closed"))
                        .onFailure(t ->
                                LOG.error("Problems closing the vertx instance",
                                t.getCause()));
            }
        });

        // connect to the MQTT Adapter
        client.connect(HONO_MQTT_ADAPTER_PORT, HONO_HOST, ch -> {
            if (ch.succeeded()) {
                LOG.info("Connected to the MQTT Adapter");
                // let's publish some messages
                timerId = vertx.setPeriodic(0, 1000, id -> {
                    String payloadString = "{\"alarm\": " + ++counter + "}";
                    publishData(payloadString);
                });
            } else {
                LOG.error("Failed to connect to the MQTT Adapter");
                LOG.error(String.valueOf(ch.cause()));
            }
        });
    }
}

