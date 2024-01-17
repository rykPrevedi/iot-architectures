package unimore.iot.architectures.tirocinio.hono.devices.http;

import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static unimore.iot.architectures.tirocinio.hono.constants.HonoConstants.*;

/**
 * Demo class that POST request for the request-response command mechanism
 * <p>
 * The Command Request contains the {@UriQueryParameter "hono-ttd"} and a {@Body NULL}.
 * The Command Response contains the {@UriQueryParameter "hono-cmd-req-id"} : /command/res/${commandRequestId}
 * or alternatively /command/res/${commandRequestId}?hono-cmd-status=${status}
 * and {@Body {"brightness-changed": true}}
 */

public class HttpCommandReqRes {

    private static final Logger LOG = LoggerFactory.getLogger(HttpCommandReqRes.class);
    private static final String USERNAME = httpDeviceAuthId + "@" + MY_TENANT_ID;
    private static final String BRIGHTNESS_CHANGED_TRUE = "{\"brightness-changed\": true}";
    private static String commandRequestId;
    private static Integer commandStatusCode;   // command status execution


    public static void main(String[] args) {

        final String uriQueryReq = "/telemetry?hono-ttd=60"; // 60 sec waiting for the res
        String baseUrl = String.format("http://%s:%d",  // http://192.168.56.18:30516
                HONO_HOST,
                HONO_HTTP_ADAPTER_PORT);

        Unirest.config().defaultBaseUrl(baseUrl);

        System.out.println("----------- command request ----------");

        Unirest
                .post(uriQueryReq)
                .basicAuth(USERNAME, devicePassword)
                .header("content-type", "application/json") // mandatory if empty body
                .asString()
                .ifSuccess(cmd -> {
                    LOG.info("\nHTTP/1.1 {} {}\n{}\n\n{}",
                            cmd.getStatus(),
                            cmd.getStatusText(),
                            cmd.getHeaders(),
                            cmd.getBody());
                    if (cmd.getHeaders().containsKey("hono-cmd-req-id") && cmd.getStatus() == 200) {
                        commandRequestId = cmd.getHeaders().getFirst("hono-cmd-req-id");
                        commandStatusCode = cmd.getStatus();
                        sendingResponseToCommand(commandRequestId, commandStatusCode);
                    } else {
                        LOG.error("No hono-cmd-req-id or 200 found in the command response");
                        LOG.error("Response can't be sent !");
                    }
                })
                .ifFailure(res -> {
                    LOG.error("Oh No, Status: {} {}", res.getStatus(), res.getStatusText());
                    LOG.error(res.getBody());
                });
    }

    private static void sendingResponseToCommand(String reqId, Integer sc) {

        System.out.println("----------- command response ----------");
        String uriQueryRes = String.format("/command/res/%s?hono-cmd-status=%d", reqId, sc);

        Unirest
                .post(uriQueryRes)
                .basicAuth(USERNAME, devicePassword)
                .header("content-type", "application/json") // set if the result of processing the command on the device is non-empty
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
