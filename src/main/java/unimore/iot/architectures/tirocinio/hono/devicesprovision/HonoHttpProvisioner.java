package unimore.iot.architectures.tirocinio.hono.devicesprovision;

import kong.unirest.Unirest;

import java.io.IOException;

import static unimore.iot.architectures.tirocinio.hono.constants.HonoConstants.*;
import static unimore.iot.architectures.tirocinio.hono.devicesprovision.UpdateTenantWithFile.updateTenant;

public class HonoHttpProvisioner extends HonoHttpProvisionerBase {

    private static final String CONFIGURATION_FILE_PATH = "C:/Users/Utente1/Desktop/architectures-iot/architectures-iot/src/main/java/unimore/iot/architectures/tirocinio/hono/tenantsetup.json";

    public static void main(String[] args) throws IOException {
        String baseUrl = String.format("http://%s:%d",  // http://192.168.56.18:30274
                HONO_HOST,
                HONO_HTTP_DEVICE_REGISTRY_PORT);
        Unirest.config().defaultBaseUrl(baseUrl);

        // create credential record and device-id
        //String deviceId = newDevice(DRM_DEVICES_API, MY_TENANT_ID);

        // set the device credentials
        //setDeviceAuthorization(DRM_CREDENTIALS_API, MY_TENANT_ID, deviceId, httpDeviceAuthId, devicePassword);

        // get all the tenants in one Object JSON
        getTenants();

        // update the tenant
        updateTenant(MY_TENANT_ID, CONFIGURATION_FILE_PATH);

    }
}
