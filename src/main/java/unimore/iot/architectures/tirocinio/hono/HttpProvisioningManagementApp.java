package unimore.iot.architectures.tirocinio.hono;

import kong.unirest.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.Constants.HonoConstants;

import java.util.function.Consumer;


/**
 * This class allows to:
 * 1. Create a new tenant
 * 2. Add a new Device to a tenant,
 * 3. Set up the credentials for the device including auth-id and password
 *
 * @author Riccardo Prevedi
 * @created 23/02/2023 - 11:21
 * @project architectures-iot
 */

public class HttpProvisioningManagementApp {

    private static final Logger LOG = LoggerFactory.getLogger(HttpProvisioningManagementApp.class);
    private static final String tenantDRMApi = "/v1/tenants/";
    private static final String deviceDRMApi = "/v1/devices/";
    private static final String credentialDRMApi = "/v1/credentials/";
    private static final String myDeviceId = "mqtt-auth-device";
    private static final String myPassword = "mypassword";
    private static final String myAuthId = "mydevice";
    private static final String mqttJsonDeviceId = "mqtt-json-device";
    private static final String mqttJsonPassword = "mqttjsonpassword";
    private static final String mqttJsonAuthId = "mqttjson";
    private static final String mqttConsumerDeviceId = "mqtt-consumer-device";
    private static final String mqttConsumerPassword = "mqttconsumerpassword";
    private static final String mqttConsumerAuthId = "mqttconsumer";

    public HttpProvisioningManagementApp() {
    }

    public static void main(String[] args) {
        String baseUrl = String.format("http://%s:%d",  // http://192.168.181.17:31735
                HonoConstants.HONO_HOST,
                HonoConstants.HONO_HTTP_DEVICE_REGISTRY_PORT);
        Unirest.config().defaultBaseUrl(baseUrl);

        //deleteTenant(tenantDRMApi, HonoConstants.MY_TENANT_ID);
        //deleteDeviceFromTenant(deviceDRMApi, HonoConstants.MY_TENANT_ID, myDeviceId);
        //deleteDeviceFromTenant(deviceDRMApi, HonoConstants.MY_TENANT_ID, mqttJsonDeviceId);
        //createTenant(tenantDRMApi, HonoConstants.MY_TENANT_ID);
        //addDeviceToTenant(deviceDRMApi, HonoConstants.MY_TENANT_ID, myDeviceId);
        //setDeviceAuthorization(credentialDRMApi, HonoConstants.MY_TENANT_ID, myDeviceId, myAuthId, myPassword);
        //addDeviceToTenant(deviceDRMApi, HonoConstants.MY_TENANT_ID, mqttJsonDeviceId);
        //setDeviceAuthorization(credentialDRMApi, HonoConstants.MY_TENANT_ID, mqttJsonDeviceId, mqttJsonAuthId, mqttJsonPassword);
        addDeviceToTenant(deviceDRMApi, HonoConstants.MY_TENANT_ID, mqttConsumerDeviceId);
        setDeviceAuthorization(credentialDRMApi, HonoConstants.MY_TENANT_ID, mqttConsumerDeviceId, mqttConsumerAuthId, mqttConsumerPassword);


    }


    /**
     * Search devices for a tenant with optional filters, paging and sorting options.
     *
     * @param resourcePath  "/v1/devices/"
     * @param tenantId      "myTenant"
     */
    public static void getDeviceByTenant(String resourcePath, String tenantId){
        Unirest
                .get(resourcePath + HonoConstants.MY_TENANT_ID)
                .header("accept", "application/json")
                .asJson()
                .ifSuccess(new Consumer<HttpResponse<JsonNode>>() {
                    @Override
                    public void accept(HttpResponse<JsonNode> httpResponse) {
                        LOG.info("Device IDs that belong to the tenant - {}:\n{}", tenantId, httpResponse.getBody().toPrettyString());
                    }
                })
                .ifFailure(new Consumer<HttpResponse<JsonNode>>() {
                    @Override
                    public void accept(HttpResponse<JsonNode> httpResponse) {
                        LOG.error("Oh No ! Status {} {}", httpResponse.getStatus(), httpResponse.getStatusText());
                        httpResponse.getParsingError().ifPresent(new Consumer<UnirestParsingException>() {
                            @Override
                            public void accept(UnirestParsingException exception) {
                                LOG.error("Parsing Exception " + exception);
                                LOG .error("Original Body: {}", exception.getOriginalBody());
                            }
                        });
                    }
                });
    }


    /**
     * Updates a device's credentials
     * <p>
     * <p>
     * curl -i -X PUT -H "content-type: application/json" --data-binary '[{
     * "type": "hashed-password",
     * "auth-id": "'${MY_DEVICE}'",
     * "secrets": [{
     * "pwd-plain": "'${MY_PWD}'"
     * }]
     * }]' http://${REGISTRY_IP}:28080/v1/credentials/${MY_TENANT}/${MY_DEVICE}
     *
     * @param resourcePath "/v1/credentials/"
     * @param tenantId     "mytenant"
     * @param deviceId     "device-mqtt-1"
     * @param authId       "auth-device-mqtt-1"
     * @param password     "mqtt-1-password"
     */
    private static void setDeviceAuthorization(String resourcePath, String tenantId, String deviceId, String authId, String password) {
        Unirest
                .put(resourcePath + tenantId + "/" + deviceId)
                .header("content-type", "application/json")
                .body(String.format("[{ \"type\": \"hashed-password\", \"auth-id\": \"%s\", \"secrets\": [{\"pwd-plain\": \"%s\" }] }]",
                        authId,
                        password))
                .asJson()
                .ifSuccess(httpResponse -> LOG.info("Password is Set !"))
                .ifFailure(httpResponse -> {
                    LOG.error("Oh No ! Status {} {}", httpResponse.getStatus(), httpResponse.getStatusText());
                    httpResponse.getParsingError().ifPresent(exception -> {
                        LOG.error("Parsing Exception " + exception);
                        LOG.error("Original Body: {}", exception.getOriginalBody());
                    });
                });
    }


    /**
     * Delete tenant
     * <p>
     * <p>
     * curl -i -X DELETE http://${REGISTRY_IP}:28080/v1/devices/${MY_TENANT}
     *
     * @param resourcePath "/v1/tenants/"
     * @param tenantId     "myTenant"
     */
    private static void deleteTenant(String resourcePath, String tenantId) {
        Unirest
                .delete(resourcePath + tenantId)
                .asEmpty()
                .ifSuccess(httpResponse -> LOG.info("{} correctly deleted !", tenantId))
                .ifFailure(httpResponse -> {
                    LOG.error("Oh No ! Status: {} {}", httpResponse.getStatus(), httpResponse.getStatusText());
                    httpResponse.getParsingError().ifPresent(exception -> {
                        LOG.error("Parsing Exception " + exception);
                        LOG.error("Original Body: {}", exception.getOriginalBody());
                    });
                });
    }

    /**
     * Create a new tenant with "myTenant" as tenant-id
     * <p>
     * <p>
     * curl -i -X POST -H "content-type: application/json" --data-binary '{
     * "ext": {
     * "messaging-type": "kafka"
     * }
     * }' http://${REGISTRY_IP}:28080/v1/tenants
     *
     * @param resourcePath "/v1/tenants/"
     * @param tenantId     "myTenant"
     */
    private static void createTenant(String resourcePath, String tenantId) {
        Unirest
                .post(resourcePath + tenantId)
                .header("content-type", "application/json")
                .body("{\"ext\": {\"messaging-type\": \"amqp\"}}")
                .asJson()
                .ifSuccess(httpResponse -> LOG.info("Registered tenant: {}", HonoConstants.MY_TENANT_ID))
                .ifFailure(httpResponse -> {
                    LOG.error("Oh No, Status: {} {}", httpResponse.getStatus(), httpResponse.getStatusText());
                    httpResponse.getParsingError().ifPresent(exception -> {
                        LOG.error("Parsing exception: " + exception);
                        LOG.error("Original body: {}", exception.getOriginalBody());
                    });
                });
    }

    /**
     * Delete an existing device registration
     * <p>
     * <p>
     * curl -i -X DELETE http://${REGISTRY_IP}:28080/v1/devices/${MY_TENANT}/${MY_DEVICE}
     *
     * @param resourcePath "/v1/devices/"
     * @param tenantId     "myTenant"
     * @param deviceId     "device-mqtt-1"
     */
    private static void deleteDeviceFromTenant(String resourcePath, String tenantId, String deviceId) {
        Unirest
                .delete(resourcePath + tenantId + "/" + deviceId)
                .asEmpty()
                .ifSuccess(httpResponse -> LOG.info("{} correctly removed from tenant - {}", deviceId, tenantId))
                .ifFailure(httpResponse -> {
                    LOG.error("Oh No ! Status {} {}", httpResponse.getStatus(), httpResponse.getStatusText());
                    httpResponse.getParsingError().ifPresent(exception -> {
                        LOG.error("Parsing Exception " + exception);
                        LOG.error("Original Body: {}", exception.getOriginalBody());
                    });
                });
    }


    /**
     * Add the device with "device-mqtt-1" as device-id.
     * This creates both a device identity and an (empty) credentials record.
     * <p>
     * <p>
     * curl -i -X POST http://${REGISTRY_IP}:28080/v1/devices/${MY_TENANT}
     *
     * @param resourcePath "/v1/devices/"
     * @param tenantId     "myTenant"
     * @param deviceId     "device-mqtt-1"
     */
    private static void addDeviceToTenant(String resourcePath, String tenantId, String deviceId) {
        Unirest
                .post(resourcePath + tenantId + "/" + deviceId)
                .header("content-type", "application/json")
                .asEmpty()
                .ifSuccess(httpResponse -> LOG.info("Registered device: {}", deviceId)).ifFailure(httpResponse -> {
                    LOG.error("Oh No, Status {} {}", httpResponse.getStatus(), httpResponse.getStatusText());
                    httpResponse.getParsingError().ifPresent(exception -> {
                        LOG.error("Parsing Exception " + exception);
                        LOG.error("Original Body: {}", exception.getOriginalBody());
                    });
                });
    }
}
