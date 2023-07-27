package unimore.iot.architectures.tirocinio.hono;

import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.Constants.HonoConstants;
import unimore.iot.architectures.tirocinio.hono.businessapplications.TemperatureNorthboundApp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static unimore.iot.architectures.tirocinio.hono.businessapplications.HttpProvisioningManagementApp.getTenants;
//import static unimore.iot.architectures.tirocinio.hono.businessapplications.HttpProvisioningManagementApp.updateTenant;

/**
 * This class instantiates all Business Applications and runs their tenant set-up
 *
 * @author Riccardo Prevedi
 * @created 06/03/2023 - 09:00
 * @project architectures-iot
 */

public class BusinessApplicationEngine {
    private static final Logger LOG = LoggerFactory.getLogger(BusinessApplicationEngine.class);
    private static final String CONFIGURATION_FILE_PATH = "C:/Users/Utente1/Desktop/architectures-iot/architectures-iot/src/main/java/unimore/iot/architectures/tirocinio/hono/tenantsetup.json";


    public static void main(String[] args) {

        String baseUrl = String.format("http://%s:%d",
                HonoConstants.HONO_HOST,
                HonoConstants.HONO_HTTP_DEVICE_REGISTRY_PORT);
        Unirest.config().defaultBaseUrl(baseUrl);

        // Get all the tenants in one Object JSON
        //getTenants();

        // First BA
        // Check the Tenant
        String tenant = "tenant-00-a1";

        // Setting-up
        try {
            updateTenant(tenant, CONFIGURATION_FILE_PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }

        TemperatureNorthboundApp temperatureNorthboundApp = new TemperatureNorthboundApp();
        temperatureNorthboundApp.setTenant(tenant);
        temperatureNorthboundApp.consumeData();
    }

    /**
     * Configuration of the tenant properties through a specified json file
     *
     * @param configurationFilePath the json file path
     */
    public static void updateTenant(String tenantId, String configurationFilePath) throws IOException {
        Unirest
                .put("/v1/tenants/" + tenantId)
                .header("content-type", "application/json")
                .body(new String(Files.readAllBytes(Paths.get(configurationFilePath))))
                .asJson()
                .ifSuccess(httpResponse -> LOG.info("Tenant [{}] Updated! Status: {} {}", tenantId, httpResponse.getStatus(), httpResponse.getStatusText()))
                .ifFailure(httpResponse -> {
                    LOG.error("Oh No ! Status {} {}", httpResponse.getStatus(), httpResponse.getStatusText());
                    LOG.error("{}", httpResponse.getBody().toPrettyString());
                });
    }
}
