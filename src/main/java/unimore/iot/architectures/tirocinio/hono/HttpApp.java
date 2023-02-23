package unimore.iot.architectures.tirocinio.hono;

import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.UnirestParsingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.Constants.HonoConstants;

import java.util.function.Consumer;

import static unimore.iot.architectures.tirocinio.hono.HttpProvisioningManagementApp.getDeviceByTenant;

/**
 * @author Riccardo Prevedi
 * @created 23/02/2023 - 17:14
 * @project architectures-iot
 */

public class HttpApp {
    private static final Logger LOG = LoggerFactory.getLogger(HttpApp.class);
    private static final String deviceDRMApi = "/v1/devices/";

    public static void main(String[] args) {
        String baseUrl = String.format("http://%s:%d",  // http://192.168.181.17:31735
                HonoConstants.HONO_HOST,
                HonoConstants.HONO_HTTP_DEVICE_REGISTRY_PORT);
        Unirest.config().defaultBaseUrl(baseUrl);

        getDeviceByTenant(deviceDRMApi, HonoConstants.MY_TENANT_ID);
    }


}
