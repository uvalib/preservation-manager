package edu.virginia.lib.aptrust.migrate;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

import java.io.FileInputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import edu.virginia.lib.aptrust.RdfConstants;
import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;
import edu.virginia.lib.aptrust.ingest.ReportFailedIngests;

/**
 * A Utility to migrate from ontology v1 to v2.  The difference is in the way 
 * failed ingests to AP Trust are represented.
 */
public class MigrateMetadataRepresentationOfIngestFailures implements RdfConstants {

    public static void main(String [] args) throws Exception {
        Properties p = new Properties();
        FileInputStream fis = new FileInputStream("ingest.properties");
        try {
            p.load(fis);
        } finally {
            fis.close();
        }
        
        FusekiReader fuseki = new FusekiReader(getRequiredProperty(p, "triplestore-url"));
        Fedora4Client f4Client = new Fedora4Client(getRequiredProperty(p, "f4-url"));
        
        // iterate over all the items to be updated
        final String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + 
                "SELECT ?object ?event\n" + 
                "WHERE {\n" + 
                "     ?object <http://fedora.lib.virginia.edu/preservation#hasFailedEvent> ?event .\n" + 
                "     ?event rdf:type <http://fedora.lib.virginia.edu/preservation#APTrustEvent> \n" + 
                "}";
        List<Map<String, String>> toMigrate = fuseki.getQueryResponse(query);
        for (Map<String, String> objectEvent : toMigrate) {
            final URI event = new URI(objectEvent.get("event"));
            final URI object= new URI(objectEvent.get("object"));
            ReportFailedIngests.markEventAsFailed(event, f4Client);

            final String addCorrect = "PREFIX premis: <" + PREMIS_NAMESPACE + ">\n"
                    + "PREFIX pres: <" + UVA_PRESERVATION_NAMESPACE + ">\n"
                    + "INSERT DATA { <> premis:hasEvent <" + event + "> }\n";
            f4Client.updateWithSparql(object, addCorrect);

            final String removeIncorrect = "PREFIX premis: <" + PREMIS_NAMESPACE + ">\n"
                    + "PREFIX pres: <" + UVA_PRESERVATION_NAMESPACE + ">\n"
                    + "DELETE DATA { <> pres:hasFailedEvent <" + event + "> }\n";
            f4Client.updateWithSparql(object, removeIncorrect);
            System.out.println("Fixed " + object + " event " + event + ".");
        }
    }
    
}
