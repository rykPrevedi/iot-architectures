package unimore.iot.architectures.tirocinio.hono.devices.http;

import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static unimore.iot.architectures.tirocinio.hono.constants.HonoConstants.*;

/**
 * Demo class that POST request with an empty body for One Way command receiving
 * the request will have a Query Parameter indicating the ttd of the device
 * <p>
 * The response (200 OK) indicating that the telemetry data has been accepted for processing.
 * The response contains a command for the device to execute.
 *  <p>
 *  Other headers:
 *  hono-command: sendLifecycleInfo
 * content-type: application/octet-stream
 * content-length: 33
 * <p>
 * Demo command received:
 * {
 *   "info" : "app restarted."
 * }
 * else, if the request failed the response body will contain the error details.
 */

public class HttpCommandOw {

    private static final Logger LOG = LoggerFactory.getLogger(HttpCommandOw.class);
    private static final String URI = "/telemetry?hono-ttd=60"; // 60 sec waiting for the res
    private static final String USERNAME = httpDeviceAuthId + "@" + MY_TENANT_ID;
    private static final String BASE_URL = String.format("http://%s:%d",
            HONO_HOST,
            HONO_HTTP_ADAPTER_PORT);

    public static void main(String[] args) {
        Unirest
                .post(BASE_URL + URI)
                .basicAuth(USERNAME, devicePassword)
                .header("content-type", "application/json") // Required, if the request body is empty
                .asString()
                .ifSuccess(cmd -> {
                    // The content-type will only be present if the response contains a command
                    LOG.info("\nHTTP/1.1 {} {}\n{}\n\n{}",
                            cmd.getStatus(),
                            cmd.getStatusText(),
                            cmd.getHeaders(),
                            cmd.getBody());
                })
                .ifFailure(res -> {
                    LOG.error("Oh No, Status: {} {}", res.getStatus(), res.getStatusText());
                    LOG.error(res.getBody());
                });
    }
}
