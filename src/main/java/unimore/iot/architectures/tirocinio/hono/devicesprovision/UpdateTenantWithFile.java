package unimore.iot.architectures.tirocinio.hono.devicesprovision;

import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;


/**
 * This class is for updating a tenant uploading its set-up by means a Json file
 */
public class UpdateTenantWithFile {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateTenantWithFile.class);

    public UpdateTenantWithFile() {// prevent instantiation
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


