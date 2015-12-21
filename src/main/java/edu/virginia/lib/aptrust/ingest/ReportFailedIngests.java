package edu.virginia.lib.aptrust.ingest;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;
import static edu.virginia.lib.aptrust.RdfConstants.PREMIS_HAS_EVENT;
import static edu.virginia.lib.aptrust.RdfConstants.PREMIS_NAMESPACE;
import static edu.virginia.lib.aptrust.RdfConstants.PRES_HAS_FAILED_EVENT;
import static edu.virginia.lib.aptrust.RdfConstants.UVA_PRESERVATION_NAMESPACE;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.fcrepo.camel.FcrepoOperationFailedException;

import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;

/**
 * Sometimes ingest to AP Trust fails after we've sent a valid package.
 * These failing must be reported to our repository in an asynchronous
 * manner so that we know those failed bags must be resent.
 *
 * This script allows a list of IDs to be marked as failed and then
 * sent again.
 * 
 * It's not clear the best way in the metadata to explain this.  Technically
 * our "ingest" premis event never occurred and we should have used another
 * value to indicate what happened (that we sent it off for ingestion).
 * 
 * Until we get a better handle on how to track these events, the asynchronous
 * process of reporting failed ingests will simply remove the premis:hasEvent
 * link to that event, and replace it with a "pres:hasFailedIngestEvent"
 * link.
 */
public class ReportFailedIngests {

    /**
     * For administrative purposes preserved content belongs to exactly one
     * container.  While these container names are part of the URI in the
     * preservation staging repository, they don't make it into the bag names
     * in AP Trust.  Therefore, for processing failed bags just using their
     * bag name requires us to attempt to locate those resources within one
     * of the known containers. 
     */
    private static final String[] KNOWN_CONTAINER = new String[] { "wsls" };
    
    public static void main(String [] args) throws IOException, URISyntaxException, FcrepoOperationFailedException {
        if (args.length != 1) {
            System.err.println("Missing argument: you must provide a filename containing the IDs of packages that failed to ingest.");
            System.exit(1);
        }
        final File f = new File(args[0]);
        if (!f.exists()) {
            System.err.println("Invalid argument: the specified file, \"" + f.getAbsolutePath() + "\",doesn't exist");
            System.exit(2);
        }
        
        Properties p = new Properties();
        FileInputStream fis = new FileInputStream("ingest.properties");
        try {
            p.load(fis);
        } finally {
            fis.close();
        }
        
        FusekiReader fuseki = new FusekiReader(getRequiredProperty(p, "triplestore-url"));
        Fedora4Client f4Client = new Fedora4Client(getRequiredProperty(p, "f4-url"));
        
        BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
        String shortId = null;
        while ((shortId = r.readLine()) != null) {
            final String url = findURL(f4Client, shortId);
            if (url == null) {
                throw new RuntimeException("No resource could be found in the repository with UUID " + shortId + "!");
            }
            
            final URI eventURI = findEventURI(url, fuseki); 
            if (eventURI == null) {
                if (findFailedEventURI(url, fuseki) != null) {
                    System.out.println("The event for " + url + " is already marked as having failed!");
                    continue;
                } else {
                    throw new RuntimeException("No event could be found in the repository for " + url + "!");
                }
            }
            markEventAsFailed(url, eventURI, f4Client);
            System.out.println("Marked " + url + " has having had a failed event " + eventURI + ".");
        }
    }
    
    /**
     * Locates a f4 resource whose UUID is the given shortID by checking 
     * within each of the known containers until it's found (or null is
     * returned).
     */
    private static String findURL(Fedora4Client f4Client, final String shortId) throws FcrepoOperationFailedException, URISyntaxException {
        String url = null;
        for (String container : KNOWN_CONTAINER) {
            final String possibleUrl = getURI(f4Client.getBaseUri().toString(), container, shortId);
            if (f4Client.exists(new URI(possibleUrl))) {
                return possibleUrl;
            }
        }
        return null;
    }
    
    private static URI findEventURI(final String subject, FusekiReader triplestore) throws URISyntaxException, IOException {
        final String response = triplestore.getFirstAndOnlyQueryResponse("select ?event where { <" + subject + "> <" + PREMIS_HAS_EVENT + "> ?event . }").get("event");
        if (response == null) {
            return null;
        }
        return new URI(response);
    }
    
    private static URI findFailedEventURI(final String subject, FusekiReader triplestore) throws URISyntaxException, IOException {
        final String response = triplestore.getFirstAndOnlyQueryResponse("select ?event where { <" + subject + "> <" + PRES_HAS_FAILED_EVENT + "> ?event . }").get("event");
        if (response == null) {
            return null;
        }
        return new URI(response);
    }
    
    private static void markEventAsFailed(final String resource, final URI event, Fedora4Client f4Client) throws UnsupportedEncodingException, FcrepoOperationFailedException, URISyntaxException {
        final String addFailed = "PREFIX premis: <" + PREMIS_NAMESPACE + ">\n"
                + "PREFIX pres: <" + UVA_PRESERVATION_NAMESPACE + ">\n"
                + "INSERT DATA { <> pres:hasFailedEvent <" + event + "> }\n";
        f4Client.updateWithSparql(new URI(resource), addFailed);
        
        final String removeFailed = "PREFIX premis: <" + PREMIS_NAMESPACE + ">\n"
                + "PREFIX pres: <" + UVA_PRESERVATION_NAMESPACE + ">\n"
                + "DELETE DATA { <> premis:hasEvent <" + event + "> }\n";
        f4Client.updateWithSparql(new URI(resource), removeFailed);

    }
    
    private static String getURI(final String baseURL, final String container, final String shortId) {
        return baseURL + "/" + container + "/" + shortId.substring(0, 2) + "/" + shortId.substring(2, 4) + "/" + shortId.substring(4, 6) + "/" + shortId.substring(6, 8) + "/" + shortId;
                
    }
}
