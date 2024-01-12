package unimore.iot.architectures.tirocinio.hono.devices.http;

import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static unimore.iot.architectures.tirocinio.hono.constants.HonoConstants.*;

/**
 * This class is a demo class
 * which allows to do a POST request with a  body {"temp": 5}
 * and the message will be sent with the default QoS "at most once" (0)
 * *
 * *
 * If the request is correct, the response will contain a message like:
 * HTTP/1.1 202 Accepted
 * content-length: 0
 * *
 * else, if the request failed the response body will contain the error details.
 */

public class HttpTelemetry {
    private static final Logger LOG = LoggerFactory.getLogger(HttpTelemetry.class);
    private static final String URI = "/telemetry";

    public static void main(String[] args) {
        String message = "{\"temp\": 5}";
        String username = httpDeviceAuthId + "@" + MY_TENANT_ID;

        Unirest
                .post("http://" + HONO_HOST + ":" + HONO_HTTP_ADAPTER_PORT + URI)
                .basicAuth(username, devicePassword)
                .header("content-type", "application/json")
                .header("qos-level", "0")
                .body(message)
                .asString()
                .ifSuccess(res -> LOG.info("\nHTTP/1.1 {} {}\n{}",
                        res.getStatus(),
                        res.getStatusText(),
                        res.getHeaders()))
                .ifFailure(res -> { LOG.error("Oh No, Status: {} {}",
                            res.getStatus(),
                            res.getStatusText());
                    LOG.error(res.getBody());
                });
    }
}
