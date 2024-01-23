package unimore.iot.architectures.tirocinio.hono.devices.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.UUID;
import java.util.regex.Pattern;

import static unimore.iot.architectures.tirocinio.hono.constants.HonoConstants.*;

/**
 * This is an Event -> Driven class
 * that receives commands on the topic for the command request-response
 * and when it has been arrived then publish the response message.
 */
public class VertxMqttCommandReqRes {
    private static final Logger LOG = LoggerFactory.getLogger(VertxMqttCommandReqRes.class);
    private static final String clientId = UUID.randomUUID().toString();
    private static final String TOPIC_REQ = "c///q/#";
    private static final Vertx vertx = Vertx.vertx();
    private static final int QOS = 0;
    private static String topicRes = null;
    private static final Integer COMMAND_STATUS_EXE_CODE = 200;
    private static final String METADATA = "/?content-type=application%2Fjson";
    private static final String BRIGHTNESS_CHANGED_TRUE = "{\"brightness-changed\": true}";
    private static final long DELAY_FOR_DISCONNECTION = 60 * 1000; // millisecond, 1 min
    private static MqttClientOptions options;

    public VertxMqttCommandReqRes() {
        options = new MqttClientOptions()
                .setUsername(mqttDeviceAuthId + "@" + MY_TENANT_ID)
                .setPassword(devicePassword)
                .setClientId(clientId);
    }

    /**
     * MAIN
     */
    public static void main(String[] args) {
        VertxMqttCommandReqRes vertxReqRes = new VertxMqttCommandReqRes();
        vertxReqRes.start();
    }

    /**
     * This method is used to split the Request Command Topic and to generate the Response Command Topic
     *
     * @param cmdTopicReply The Command Request Topic : c///q/${req-id}/${command}
     * @return The Command Response Topic :
     */
    private static String composeCommandTopicRes(String cmdTopicReply) {
        String cmdReqId;
        String[] output = cmdTopicReply.split(Pattern.quote("/"));
        cmdReqId = output[4];
        return String.format("c///s/%s/%d", cmdReqId, COMMAND_STATUS_EXE_CODE);
    }

    public void start() {
        MqttClient client = MqttClient.create(vertx, options);
        // received the message -> activate publication
        client.publishHandler(publish -> {
            System.out.println("-------------------------------------------------");
            LOG.info("Just received message with QoS [{}]", publish.qosLevel());
            System.out.println("-------------------------------------------------");
            System.out.println("| Topic:" + publish.topicName());
            System.out.println("| Message: " + publish.payload().toString(Charset.defaultCharset()));
            System.out.println("-------------------------------------------------");
            topicRes = composeCommandTopicRes(publish.topicName());
            client.publish(
                    topicRes + METADATA,
                    Buffer.buffer(BRIGHTNESS_CHANGED_TRUE),
                    MqttQoS.AT_MOST_ONCE,
                    false,
                    false,
                    s -> LOG.info("Publish sent to the Adapter"));
            // unsubscribe from receiving messages for earlier subscribed topic
            vertx.setTimer(DELAY_FOR_DISCONNECTION, l -> client.unsubscribe(TOPIC_REQ));
        });
        // handle response on subscribe request
        client.subscribeCompletionHandler(h -> {
            LOG.info("Receive SUBACK from the MQTT Adapter with granted QoS : {}",
                    h.grantedQoSLevels());
        });
        // handle response on unsubscribe request
        client.unsubscribeCompletionHandler(h -> {
            LOG.info("Receive UNSUBACK from the MQTT Adapter");
            // disconnect from Adapter
            client.disconnect(d -> LOG.info("Disconnected from the Adapter"));
            vertx.close()
                    .onSuccess(ok -> LOG.info("Vertx instance closed"))
                    .onFailure(t -> LOG.error("Problems closing the vertx instance",
                            t.getCause()));
        });
        // connect to the MQTT Adapter
        client.connect(HONO_MQTT_ADAPTER_PORT, HONO_HOST, ch -> {
            if (ch.succeeded()) {
                LOG.info("Connected to the MQTT Adapter");
                client.subscribe(TOPIC_REQ, QOS);
            } else {
                LOG.error("Failed to connect to the MQTT Adapter");
                LOG.error(String.valueOf(ch.cause()));
            }
        });
    }
}
