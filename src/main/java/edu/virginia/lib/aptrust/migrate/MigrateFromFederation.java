package edu.virginia.lib.aptrust.migrate;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getOptionalProperty;
import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.fcrepo.client.FcrepoOperationFailedException;

import edu.virginia.lib.aptrust.RdfConstants;
import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;

public class MigrateFromFederation {
    
    public static void main(String [] args) throws Exception {
        Properties p = new Properties();
        FileInputStream fis = new FileInputStream("production-ingest.properties");
        try {
            p.load(fis);
        } finally {
            fis.close();
        }
        
        FusekiReader fuseki = new FusekiReader(getRequiredProperty(p, "triplestore-url"));
        Fedora4Client f4Client = new Fedora4Client(getOptionalProperty(p, "f4-username"), getOptionalProperty(p, "f4-password"), getRequiredProperty(p, "f4-url"));
        
        migrateFromFederation(f4Client, fuseki);        
    }
    
    public static void migrateFromFederation(Fedora4Client f4, FusekiReader fuseki) throws IOException, URISyntaxException, FcrepoOperationFailedException {
        long offset = 0l;
        int pageSize = 5;
        List<Map<String, String>> queryResult = null;
        do {
            String query = "PREFIX pres: <http://fedora.lib.virginia.edu/preservation#>\n" + 
                    "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + 
                    "\n" + 
                    "SELECT DISTINCT ?file ?binary ?federationUrl\n" + 
                    "WHERE {\n" + 
                    "   ?file pres:hasBinary ?binary .\n" + 
                    "   ?file dc:identifier ?federationUrl .\n" + 
                    "} ORDER BY ?federationUrl LIMIT " + pageSize + " OFFSET " + offset;
            System.out.println("Requesting " + pageSize + " results from offset " + offset + "...");
            queryResult = fuseki.getQueryResponse(query);
            for (Map<String, String> result : queryResult) {
                final URI fileUri = new URI(result.get("file"));
                final URI binaryURI = new URI(result.get("binary"));
                final URI binaryMetadataURI = new URI(binaryURI.toString() + "/fcr:metadata");
                final String id = result.get("federationUrl");
                
                final String newId = getNewFederationFileId(id);
                if (newId.equals(id) && !"".equals(f4.getSingleRequiredPropertyValue(binaryMetadataURI, binaryURI, RdfConstants.FILENAME))) {
                    System.out.println(fileUri + " skipped");
                } else {
                    System.out.println(fileUri + " (" + id + " --> " + newId);
                    f4.updateLiteralProperty(fileUri, RdfConstants.DC_IDENTIFIER, newId);
                    f4.updateRedirectNonRDFResource(newId, binaryURI);
                    f4.addLiteralProperty(binaryMetadataURI, RdfConstants.FILENAME, getFilename(newId));
                }    
            }
            offset += pageSize;
        } while (queryResult.size() == pageSize);
    }
    
    private static String getNewFederationFileId(String originalId) {
        return originalId.replace(":8080/fcrepo/rest/av-masters/", "/av-masters/");
    }
    
    private static String getFilename(String id) {
        return id.substring(id.lastIndexOf('/') + 1);
    }
    
}
