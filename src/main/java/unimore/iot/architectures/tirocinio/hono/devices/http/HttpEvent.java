package unimore.iot.architectures.tirocinio.hono.devices.http;

import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static unimore.iot.architectures.tirocinio.hono.constants.HonoConstants.*;

/**
 * Demo class that POST a request with body {"alarm": "fire"}
 * the message will be sent with a ttl (10 sec)
 * and the message will be sent with the  QoS "at least once" (1)
 * <p>
 * If the request is correct, the response will contain a message like:
 * HTTP/1.1 202 Accepted
 * content-length: 0
 * <p>
 * else, if the request failed the response body will contain the error details.
 */

public class HttpEvent {
    private static final Logger LOG = LoggerFactory.getLogger(HttpEvent.class);
    private static final String URI_PATH = "/event";
    private static final String USERNAME = httpDeviceAuthId + "@" + MY_TENANT_ID;
    private static final String BASE_URL = String.format("http://%s:%d",
            HONO_HOST,
            HONO_HTTP_ADAPTER_PORT);

    public static void main(String[] args) {
        String message = "{\"alarm\": \"fire\"}";

        Unirest
                .post(BASE_URL + URI_PATH)
                .basicAuth(USERNAME, devicePassword)
                .header("content-type", "application/json")
                .header("qos-level", "1")
                .header("hono-ttl", "10")
                .body(message)
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
}
