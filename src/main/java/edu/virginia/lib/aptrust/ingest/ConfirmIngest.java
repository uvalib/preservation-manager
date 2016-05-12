package edu.virginia.lib.aptrust.ingest;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getOptionalProperty;
import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

import java.io.FileInputStream;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import javax.json.JsonObject;

import edu.virginia.lib.aptrust.RdfConstants;
import edu.virginia.lib.aptrust.helper.APTrustAPIHelper;
import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;

/**
 * A script that identifies all ingests for which outcome information can be verified but hasn't
 * and queries the APTrust API to determine that outcome information.
 */
public class ConfirmIngest implements RdfConstants {
    
    public static void main(String [] args) throws Exception {
        Properties p = new Properties();
        FileInputStream fis = new FileInputStream("ingest.properties");
        try {
            p.load(fis);
        } finally {
            fis.close();
        }
        
        FusekiReader fuseki = new FusekiReader(getRequiredProperty(p, "triplestore-url"));
        Fedora4Client f4Client = new Fedora4Client(getOptionalProperty(p, "f4-username"), getOptionalProperty(p, "f4-password"), getRequiredProperty(p, "f4-url"));
        APTrustAPIHelper aptrust = new APTrustAPIHelper(getRequiredProperty(p, "aptrust-api-url"), getRequiredProperty(p, "aptrust-api-key"), getRequiredProperty(p, "aptrust-api-user"));
        
        // walk through all ingest events that have etags but no eventOutcomeInformation and query the 
        // API to determine the status of their ingest
        
        final String query = "PREFIX pres: <http://fedora.lib.virginia.edu/preservation#>\n" + 
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
                "PREFIX premis: <http://www.loc.gov/premis/rdf/v1#>\n" + 
                "\n" + 
                "SELECT ?ingestEvent ?package ?etag\n" + 
                "WHERE {\n" + 
                "     {\n" + 
                "       ?package premis:hasEvent ?ingestEvent .\n" + 
                "       ?ingestEvent rdf:type <http://fedora.lib.virginia.edu/preservation#APTrustEvent> .\n" + 
                "       ?ingestEvent premis:hasEventType <http://id.loc.gov/vocabulary/preservationEvents/ingestion> .\n" + 
                "       ?ingestEvent pres:aptrustEtag ?etag\n" + 
                "     } MINUS {\n" + 
                "       ?ingestEvent premis:hasEventOutcomeInformation ?outcome\n" + 
                "     }    \n" + 
                "}";
        
        for (Map<String, String> result : fuseki.getQueryResponse(query)) {
            final String event = result.get("ingestEvent");
            final String packageUri = result.get("package");
            final String name = "virginia.edu." + packageUri.substring(packageUri.lastIndexOf('/') + 1) + ".tar";
            final String etag = result.get("etag");
            System.out.println("Checking ingest outcome for " + name + "...");
            JsonObject report = aptrust.getIngestReportForResource(name, etag);
            if (report == null) {
                System.out.println("No record of " + name + " in AP Trust!");
                // TODO: add some error handling if it's been beyond a certain length of time
            } else {
                final String status = report.getString("status");
                if (status.equals("Success")) {
                    markEventAsSuccess(new URI(event), f4Client);
                    System.out.println("Success!");
                } else if (status.equals("Failed")) {
                    if (report.getString("retry").equals("true")) {
                        System.out.println("Failed, but maybe not forever...");
                    } else {
                        final String reason = report.getString("note");
                        markEventAsFailed(new URI(event), f4Client);
                        System.out.println("Failed!  " + reason);
                    }
                } else {
                    System.out.println(name + " has status " + status + "...");
                    // TODO: add some error handling if it's been beyond a certain length of time
                }
            }
            
        }
        
    }
    
    public static void markEventAsFailed(final URI event, Fedora4Client f4Client) throws Exception {
        if (f4Client.getPropertyValues(event, event, PREMIS_HAS_EVENT_OUTCOME_INFORMATION).isEmpty()) {
            final URI outcome = f4Client.createResource(event.toString());
            try {
                f4Client.addURIProperty(outcome, RDF_TYPE, new URI(AP_TRUST_EVENT_OUTCOME_INFORMATION));
                f4Client.addLiteralProperty(outcome, PREMIS_HAS_EVENT_OUTCOME, "failure");
                f4Client.addURIProperty(event, PREMIS_HAS_EVENT_OUTCOME_INFORMATION, outcome);
            } catch (RuntimeException ex) {
                System.err.println("Error while updating new event outcome " + outcome + "!");
                throw ex;
            } catch (Exception ex) {
                System.err.println("Error while updating new event outcome " + outcome + "!");
                throw ex;
            }
        } else {
            throw new RuntimeException("Event " + event + " already has a reported outcome!");
        }
    }
    
    public static void markEventAsSuccess(final URI event, Fedora4Client f4Client) throws Exception {
        if (f4Client.getPropertyValues(event, event, PREMIS_HAS_EVENT_OUTCOME_INFORMATION).isEmpty()) {
            final URI outcome = f4Client.createResource(event.toString());
            try {
                f4Client.addURIProperty(outcome, RDF_TYPE, new URI(AP_TRUST_EVENT_OUTCOME_INFORMATION));
                f4Client.addLiteralProperty(outcome, PREMIS_HAS_EVENT_OUTCOME, "success");
                f4Client.addURIProperty(event, PREMIS_HAS_EVENT_OUTCOME_INFORMATION, outcome);
            } catch (RuntimeException ex) {
                System.err.println("Error while updating new event outcome " + outcome + "!");
                throw ex;
            } catch (Exception ex) {
                System.err.println("Error while updating new event outcome " + outcome + "!");
                throw ex;
            }
        } else {
            throw new RuntimeException("Event " + event + " already has a reported outcome!");
        }
    }

}
