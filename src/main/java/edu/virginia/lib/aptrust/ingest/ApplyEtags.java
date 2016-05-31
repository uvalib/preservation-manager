package edu.virginia.lib.aptrust.ingest;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getOptionalProperty;
import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.fcrepo.client.FcrepoOperationFailedException;

import edu.virginia.lib.aptrust.RdfConstants;
import edu.virginia.lib.aptrust.helper.APTrustAPIHelper;
import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;

/**
 * A script to hit the AP Trust API to find the etags for ingest operations 
 * given the following assumptions.  For all resources with a single AP Trust
 * event that has no etag listed where there's also exactly one ingest action
 * registered in AP Trust, assume they match and apply the etag to the resource
 * in fedora 4.  
 */
public class ApplyEtags {

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
        
        // walk through all events that have etags but no eventOutcomeInformation and query the 
        // API to determine the status of their ingest
        
        final String query = "PREFIX pres: <http://fedora.lib.virginia.edu/preservation#>\n" + 
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
                "PREFIX premis: <http://www.loc.gov/premis/rdf/v1#>\n" + 
                " \n" + 
                "SELECT ?package ?ingestEvent\n" + 
                "WHERE {\n" + 
                "     {\n" + 
                "       ?package premis:hasEvent ?ingestEvent .\n" + 
                "       ?ingestEvent rdf:type <http://fedora.lib.virginia.edu/preservation#APTrustEvent> .\n" + 
                "       ?ingestEvent premis:hasEventType <http://id.loc.gov/vocabulary/preservationEvents/ingestion> .\n" + 
                "     } MINUS {\n" + 
                "       ?ingestEvent pres:aptrustEtag ?etag\n" + 
                "     }   \n" + 
                "} LIMIT 1000";
        List<Map<String, String>> batch = fuseki.getQueryResponse(query);
        boolean processed = true;
        while (processed) {
            processed = false;
            for (Map<String, String> result : fuseki.getQueryResponse(query)) {
                final String event = result.get("ingestEvent");
                final String packageUri = result.get("package");
                final String name = "virginia.edu." + packageUri.substring(packageUri.lastIndexOf('/') + 1) + ".tar";
                System.out.println("Checking ingest outcome for " + name + "...");
                JsonArray reports = aptrust.getIngestReportsForResource(name);
                if (reports == null || reports.size() == 0) {
                    System.out.println("No record of " + name + " in AP Trust!");
                } else if (reports.size() == 1) {
                    JsonObject report = reports.getJsonObject(0);
                    final String etag = report.getString("etag");
                    assignEtagAndWait(f4Client, fuseki, event, etag);
                    final String status = report.getString("status");
                    updateEventStatus(f4Client, status, event, report);
                    processed = true;
                } else {
                    System.out.println(name + " has " + reports.size() + " ingest events in AP Trust!");
                    final String orderedEventQuery = "PREFIX pres: <http://fedora.lib.virginia.edu/preservation#>\n" + 
                            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
                            "PREFIX premis: <http://www.loc.gov/premis/rdf/v1#>\n" + 
                            " \n" + 
                            "SELECT ?ingestEvent ?date\n" + 
                            "WHERE {\n" + 
                            "       <" + packageUri + "> premis:hasEvent ?ingestEvent .\n" + 
                            "       ?ingestEvent rdf:type <http://fedora.lib.virginia.edu/preservation#APTrustEvent> .\n" + 
                            "       ?ingestEvent premis:hasEventType <http://id.loc.gov/vocabulary/preservationEvents/ingestion> .     \n" + 
                            "       ?ingestEvent premis:hasEventDateTime ?date\n" + 
                            "} ORDER BY ?date";
                    final List<Map<String, String>> orderedEvents = fuseki.getQueryResponse(orderedEventQuery);
                    if (orderedEvents.size() == reports.size()) {
                        ArrayList<JsonObject> r = new ArrayList<JsonObject>();
                        for (int i = 0; i < reports.size(); i ++) {
                            r.add(reports.getJsonObject(i));
                        }
                        Collections.sort(r, new Comparator() {
                            @Override
                            public int compare(Object arg0, Object arg1) {
                                return (((JsonObject) arg0).getString("created_at").compareTo(((JsonObject) arg1).getString("created_at")));
                            }});
                        for (int i = 0; i < reports.size(); i ++) {
                            String currentEvent = orderedEvents.get(i).get("ingestEvent");
                            final JsonObject report = r.get(i);
                            final String etag = report.getString("etag");
                            assignEtagAndWait(f4Client, fuseki, currentEvent, etag);
                            final String status = report.getString("status");
                            try {
                                updateEventStatus(f4Client, status, currentEvent, report);
                            } catch (RuntimeException ex) {
                                if (ex.getMessage().contains("already has a reported outcome")) {
                                    System.out.println(ex.getMessage());
                                } else {
                                    throw ex;
                                }
                            }
                        }
                    } else {
                        System.out.println(name + " has " + reports.size() + " ingests events at AP Trust, but " + orderedEvents.size() + " events in fedora!");
                    }
                    
                }
                
            }
            batch = fuseki.getQueryResponse(query);
        }        
    }
    
    private static void assignEtagAndWait(final Fedora4Client f4Client, final FusekiReader fuseki, final String event, final String etag) throws FcrepoOperationFailedException, URISyntaxException, IOException, InterruptedException {
        f4Client.addLiteralProperty(new URI(event), RdfConstants.PRES_BAG_ID, etag);
        // wait for propagation...
        final String hasEtagQuery = "PREFIX pres: <http://fedora.lib.virginia.edu/preservation#>\n" + 
                " \n" + 
                "SELECT ?etag\n" + 
                "WHERE {\n" + 
                "     <" + event + "> pres:aptrustEtag ?etag\n" + 
                "}";
        while (fuseki.getQueryResponse(hasEtagQuery).size() != 1) {
            System.out.println("Waiting for index update to propagate...");
            Thread.sleep(250);
        }
    }
    
    private static void updateEventStatus(final Fedora4Client f4Client, final String status, final String event, final JsonObject report) throws URISyntaxException, Exception {
        if (status.equals("Success")) {
            ConfirmIngest.markEventAsSuccess(new URI(event), f4Client);
            System.out.println("Success!");
        } else if (status.equals("Failed")) {
            if (report.getBoolean("retry")) {
                System.out.println("Failed, but maybe not forever...");
            } else {
                final String reason = report.getString("note");
                ConfirmIngest.markEventAsFailed(new URI(event), f4Client);
                System.out.println("Failed!  " + reason);
            }
        } else {
            System.out.println("Unexpected status " + status + "...");
            // TODO: add some error handling if it's been beyond a certain length of time
        }
    }
}
