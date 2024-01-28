package unimore.iot.architectures.tirocinio.hono.devices.http;

import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static unimore.iot.architectures.tirocinio.hono.constants.HonoConstants.*;

/**
 * Demo class that POST a telemetry message with {@UriQueryParameter "hono-ttd"} and a {@Body NULL}.
 * The Request for the command
 * <p>
 * POST a message with {@UriQueryParameter "hono-cmd-req-id"} : /command/res/${commandRequestId}?hono-cmd-status=${status}
 * The Response to the command
 */

public class HttpCommandConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(HttpCommandConsumer.class);
    private static final String USERNAME = httpDeviceAuthId + "@" + MY_TENANT_ID;
    private static final String BASE_URL = String.format("http://%s:%d",
            HONO_HOST,
            HONO_HTTP_ADAPTER_PORT);
    private static final String URI_QUERY_REQ = "/telemetry?hono-ttd=60";
    private static final String BRIGHTNESS_CHANGED_TRUE = "{\"brightness-changed\": true}";
    private static final Integer MESSAGE_COUNT = 10;
    private static final Integer PERIOD = 5000; // ms

    public static void main(String[] args) throws InterruptedException {
        Unirest.config().defaultBaseUrl(BASE_URL);
        for (int i = 0; i<MESSAGE_COUNT; i++){
            sendingRequestForCommand();
            Thread.sleep(PERIOD);
        }
    }

    private static void sendingRequestForCommand(){
        System.out.println("\n----------- command request ----------\n");
        Unirest
                .post(URI_QUERY_REQ)
                .basicAuth(USERNAME, devicePassword)
                .header("content-type", "application/json") // mandatory if empty body
                .asString()
                .ifSuccess(cmd -> {
                    LOG.info("HTTP/1.1 {} {}",
                            cmd.getStatus(),
                            cmd.getStatusText());
                    handleCommand(cmd);
                })
                .ifFailure(res -> {
                    LOG.error("Oh No, Status: {} {}", res.getStatus(), res.getStatusText());
                    LOG.error(res.getBody());
                });
    }

    private static void handleCommand(HttpResponse<String> c) {
        String subject = c.getHeaders().getFirst("hono-command");
        String commandPayload = c.getBody();
        if (!c.getHeaders().containsKey("hono-cmd-req-id")) {
            // one-way command
            LOG.info("Received one-way command [name : {}]: {}", subject, commandPayload);
        } else {
            // request-response command
            LOG.info("Received command [name : {}]: {}", subject, commandPayload);
            sendingResponseToCommand(c.getHeaders().getFirst("hono-cmd-req-id"), c.getStatus());
        }
    }

    private static void sendingResponseToCommand(String reqId, Integer sc) {
        System.out.println("\n----------- command response ----------\n");
        String uriQueryRes = String.format("/command/res/%s?hono-cmd-status=%d", reqId, sc);
        Unirest
                .post(uriQueryRes)
                .basicAuth(USERNAME, devicePassword)
                .header("content-type", "application/json")
                .header("hono-cmd-req-id", reqId)
                .body(BRIGHTNESS_CHANGED_TRUE)
                .asJson()
                .ifSuccess(jsonCmdRes -> LOG.info("\nHTTP/1.1 {} {}",
                        jsonCmdRes.getStatus(),
                        jsonCmdRes.getStatusText()))
                .ifFailure(jsonCmdErr -> {
                    LOG.error("Oh No, Status: {} {}", jsonCmdErr.getStatus(), jsonCmdErr.getStatusText());
                    LOG.error(String.valueOf(jsonCmdErr.getBody()));
                });
    }
}
