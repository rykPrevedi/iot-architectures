package unimore.iot.architectures.tirocinio.hono;

import kong.unirest.Unirest;
import unimore.iot.architectures.tirocinio.hono.Constants.HonoConstants;
import unimore.iot.architectures.tirocinio.hono.businessapplications.AvgTemperatureNorthboundApp;

import java.io.IOException;

import static unimore.iot.architectures.tirocinio.hono.businessapplications.HttpProvisioningManagementApp.getTenants;
import static unimore.iot.architectures.tirocinio.hono.businessapplications.HttpProvisioningManagementApp.updateTenant;

/**
 * @author Riccardo Prevedi
 * @created 06/03/2023 - 09:00
 * @project architectures-iot
 */

public class BusinessApplicationEngine {
    private static final String CONFIGURATION_FILE_PATH = "src/main/java/unimore/iot/architectures/tirocinio/hono/firstTenantSetup.json";


    public static void main(String[] args) {

        String baseUrl = String.format("http://%s:%d",
                HonoConstants.HONO_HOST,
                HonoConstants.HONO_HTTP_DEVICE_REGISTRY_PORT);
        Unirest.config().defaultBaseUrl(baseUrl);

        // Get all the tenants in one Object JSON
        getTenants();

        // First BA
        // Check the Tenant
        String tenant = "mytenant";

        // Setting-up
        try {
            updateTenant(tenant, CONFIGURATION_FILE_PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }

        AvgTemperatureNorthboundApp northboundApp = new AvgTemperatureNorthboundApp();
        northboundApp.setTenant(tenant);
        northboundApp.consumeData();

    }
}
