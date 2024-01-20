package unimore.iot.architectures.tirocinio.hono;

import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unimore.iot.architectures.tirocinio.hono.businessapplications.DemoSolution;
import unimore.iot.architectures.tirocinio.hono.constants.HonoConstants;
//import static unimore.iot.architectures.tirocinio.hono.businessapplications.HttpProvisioningManagementApp.updateTenant;

/**
 * This class is the place where the Business Applications are executed
 *
 *
 * @author Riccardo Prevedi
 * @created 06/03/2023 - 09:00
 * @project architectures-iot
 */

public class BusinessApplicationEngine {
    private static final Logger LOG = LoggerFactory.getLogger(BusinessApplicationEngine.class);

    public static void main(String[] args) {

        String baseUrl = String.format("http://%s:%d",
                HonoConstants.HONO_HOST,
                HonoConstants.HONO_HTTP_DEVICE_REGISTRY_PORT);
        Unirest.config().defaultBaseUrl(baseUrl);

        // First BA
        // Check the Tenant
        //String tenant = "tenant-00-a1";
        //TemperatureApp temperatureApp = new TemperatureApp();
        //temperatureApp.setTenant(tenant);
        //temperatureApp.consumeData();

        // second BA
        DemoSolution solution = new DemoSolution();
        solution.consumeData();


    }
}
