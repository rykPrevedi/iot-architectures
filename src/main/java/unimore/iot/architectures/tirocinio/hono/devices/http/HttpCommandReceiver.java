package unimore.iot.architectures.tirocinio.hono.devices.http;

import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static unimore.iot.architectures.tirocinio.hono.constants.HonoConstants.*;

/**
 * This class is a demo class
 * which allows to do a POST request with an empty body and {@header hono-ttd}
 * <p>
 * The response:
 * HTTP/1.1 200 OK
 * hono-command: sendLifecycleInfo
 * content-type: application/octet-stream
 * content-length: 33
 * <p>
 *  body.....
 *  <p>
 * else, if the request failed the response body will contain the error details.
 */

public class HttpCommandReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(HttpCommandReceiver.class);
    private static final String URI = "/telemetry";

    public static void main(String[] args) {
        String username = httpDeviceAuthId + "@" + MY_TENANT_ID;

        Unirest
                .post("http://" + HONO_HOST + ":" + HONO_HTTP_ADAPTER_PORT + URI)
                .basicAuth(username, devicePassword)
                .header("content-type", "application/json") // Required, if the request body is empty
                .header("hono-ttd", "10")                   // number of seconds the device will wait for the response.
                .asString()
                .ifSuccess(cmd -> {
                    // The content-type will only be present if the response contains a command
                    LOG.info("\nHTTP/1.1 {} {}\n{}\n\n{}", cmd.getStatus(), cmd.getStatusText(), cmd.getHeaders(), cmd.getBody());
                })
                .ifFailure(res -> {
                    LOG.error("Oh No, Status: {} {}", res.getStatus(), res.getStatusText());
                    LOG.error(res.getBody());
                });
    }
}
