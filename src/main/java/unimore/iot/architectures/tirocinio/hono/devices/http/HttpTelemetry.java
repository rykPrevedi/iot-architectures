package unimore.iot.architectures.tirocinio.hono.devices.http;

import com.google.gson.Gson;
import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.model.MessageDescriptor;

import static unimore.iot.architectures.tirocinio.hono.constants.HonoConstants.*;

/**
 * Demo class that POST 10 requests every 5 seconds with a JSON serialized body
 * The default QoS granted is "at most once" (0)
 * <p>
 * If the request is correct, the response will contain a message like:
 * HTTP/1.1 202 Accepted
 * content-length: 0
 * <p>
 * else, if the request failed the response body will contain the error details.
 */

public class HttpTelemetry {
    private static final Logger LOG = LoggerFactory.getLogger(HttpTelemetry.class);
    private static final String URI_PATH = "/telemetry";
    private static final String USERNAME = httpDeviceAuthId + "@" + MY_TENANT_ID;
    private static final String BASE_URL = String.format("http://%s:%d",
            HONO_HOST,
            HONO_HTTP_ADAPTER_PORT);
    private static final Integer MESSAGE_COUNT = 10;
    private static final Integer PERIOD = 5000; // ms
    private static final String QOS = "0";

    public HttpTelemetry() {}

    public static void main(String[] args) throws InterruptedException {
        double sensorValue = 5;
        String jsonString = buildJsonMessage(sensorValue);
        HttpTelemetry httpTelemetry = new HttpTelemetry();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            httpTelemetry.publishHttpData(jsonString);
            Thread.sleep(PERIOD);
        }
    }

    private void publishHttpData(String b) {
        Unirest
                .post(BASE_URL + URI_PATH)
                .basicAuth(USERNAME, devicePassword)
                .header("content-type", "application/json")
                .header("qos-level", QOS)
                .body(b)
                .asString()
                .ifSuccess(r -> LOG.info("\nHTTP/1.1 {} {}",
                        r.getStatus(),
                        r.getStatusText()))
                .ifFailure(e -> {
                    LOG.error("Oh No, Status: {} {}",
                            e.getStatus(),
                            e.getStatusText());
                    LOG.error(e.getBody());
                });
    }

    private static String buildJsonMessage(double v) {
        try {
            Gson gson = new Gson();
            MessageDescriptor messageDescriptor =
                    new MessageDescriptor(MessageDescriptor.HTTP_SENSOR_VALUE, v);
            return gson.toJson(messageDescriptor);
        } catch (Exception e) {
            LOG.error("Error creating json payload ! Message: {}",
                    e.getLocalizedMessage());
            return null;
        }
    }
}
