package edu.virginia.lib.aptrust.bag;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getProperties;
import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;
import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getOptionalProperty;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.fcrepo.client.FcrepoOperationFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import edu.virginia.lib.aptrust.RdfConstants;
import edu.virginia.lib.aptrust.bags.APTrustInfo;
import edu.virginia.lib.aptrust.bags.BagInfo;
import edu.virginia.lib.aptrust.bags.BagSummary;
import edu.virginia.lib.aptrust.bags.util.BagSubmitter;
import edu.virginia.lib.aptrust.bags.util.BagSubmitter.TransferSummary;
import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;

public class Bagger {

    final private static Logger LOGGER = LoggerFactory.getLogger(Bagger.class);

    private static final boolean OVERWRITE = false;

    public static void main(String[] args) throws Exception {
        Bagger b = new Bagger();
    }

    private Fedora4Client f4Client;

    private FusekiReader triplestore;

    private BagSubmitter submitter;

    public Bagger() throws Exception {
        Properties ingestProperties = getProperties("ingest.properties");

        f4Client = new Fedora4Client(getOptionalProperty(ingestProperties, "f4-username"), getOptionalProperty(ingestProperties, "f4-password"), getRequiredProperty(ingestProperties, "f4-url"));
        triplestore = new FusekiReader(getRequiredProperty(ingestProperties, "triplestore-url"));

        Properties p = getProperties("aws-credentials.properties");
        AWSCredentials credentials = new BasicAWSCredentials(getRequiredProperty(p, "accessKey"), getRequiredProperty(p, "secretKey"));
        AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);

        submitter = new BagSubmitter(amazonS3Client, getRequiredProperty(p, "bucketName"));

        ingestLibra();

    }
    
    private void ingestLibra() throws Exception {
        // create bag
        final URI uri = new URI("http://fedora01.lib.virginia.edu:8080/fcrepo/rest/libra");
        Fedora4APTrustBag bag = new Fedora4APTrustBag(new BagInfo().sourceOrganization("virginia.edu"),
                new APTrustInfo("Libra: Online Archive of University of Virginia Scholarship", APTrustInfo.CONSORTIA), uri, f4Client, triplestore);
        LOGGER.debug("Creating and transferring bag for " + uri + " at " + new Date() + "...");
        BagSummary bs = bag.serializeAPTrustBag(new File("output"), true);
        LOGGER.debug(bs.getManifestCopy());
        LOGGER.info(bs.getFile().getName() + " created, " + bs.getFile().length()
                + " bytes with base64 checksum=" + bs.getBase64Checksum());
        
        // ingest bag
        BagSubmitter.TransferSummary ts = submitter.transferBag(bs, OVERWRITE);
        if (ts.wasTransferred()) {
            createPremisEventForIngest(f4Client, triplestore, uri, bs, ts);
            bs.getFile().delete();
            LOGGER.info("Transferred in " + ts.getDuration() + " ms.");
        } else {
            LOGGER.warn(bs.getFile() + " not transferred!  " + ts.getMessage());
            bs.getFile().delete();
        }
    }
    
    private void ingestWSLS(long quota) throws Exception {
        long currentUsage = getPayloadBytesSubmitted();
        LOGGER.info(currentUsage + " of " + quota + " bytes used.");
        List<Map<String, String>> resultPage = getNextPageOfWSLSResultsToSubmit(100);
        if (resultPage.size() == 0) {
            System.out.println("No items need to be sent to AP Trust.");
        }
        while (resultPage != null && resultPage.size() > 0) {
            for (Map<String, String> r : resultPage) {
                final String uri = r.get("s");
                if (hasBeenSent(uri)) {
                    LOGGER.warn("Skipping " + r.get("wslsid") + " " + uri + " since it was already sent!");
                } else {
                    try {
                        LOGGER.info("Processing " + r.get("wslsid") + " " + uri);
                        final String title = Fedora4Client.getFirstPropertyValue(
                                f4Client.getAllProperties(new URI(uri + "/fcr:metadata")), new URI(uri),
                                RdfConstants.DC_TITLE);
                        Fedora4APTrustBag bag = new Fedora4APTrustBag(new BagInfo().sourceOrganization("virginia.edu"),
                                new APTrustInfo(title, APTrustInfo.CONSORTIA), new URI(uri), f4Client, triplestore);

                        LOGGER.debug("Creating and transferring bag for " + uri + " at " + new Date() + "...");
                        BagSummary bs = bag.serializeAPTrustBag(new File("output"), true);
                        LOGGER.debug(bs.getManifestCopy());
                        LOGGER.info(bs.getFile().getName() + " created, " + bs.getFile().length()
                                + " bytes with base64 checksum=" + bs.getBase64Checksum());
                        if (bs.getBagPayloadSize() + currentUsage > quota) {
                            LOGGER.info("Quota reached.");
                            return;
                        }
                        // check bag size vs. current usage
                        BagSubmitter.TransferSummary ts = submitter.transferBag(bs, OVERWRITE);
                        if (ts.wasTransferred()) {
                            createPremisEventForIngest(f4Client, triplestore, new URI(uri), bs, ts);
                            currentUsage += bs.getBagPayloadSize();
                            bs.getFile().delete();
                            LOGGER.info("Transferred in " + ts.getDuration() + " ms (" + currentUsage
                                    + " bytes used of " + quota + ")");
                        } else {
                            LOGGER.warn(bs.getFile() + " not transferred!  " + ts.getMessage());
                            bs.getFile().delete();
                        }
                    } catch (RuntimeException ex) {
                        LOGGER.error("Error with " + uri + ", skipping it... ", ex);
                    }
                }
            }
            resultPage = getNextPageOfWSLSResultsToSubmit(100);
        }
    }

    public static void createPremisEventForIngest(final Fedora4Client f4Client, final FusekiReader triplestore, URI uri, BagSummary bs, TransferSummary ts)
            throws FcrepoOperationFailedException, URISyntaxException, IOException {
        final URI eventURI = f4Client.createResource(uri.toString());
        LOGGER.info("Created event resource " + uri);
        f4Client.addURIProperty(eventURI, RdfConstants.RDF_TYPE,
                new URI(RdfConstants.AP_TRUST_PRESERVATION_EVENT_TYPE));
        f4Client.addURIProperty(eventURI, RdfConstants.RDF_TYPE, new URI(RdfConstants.PREMIS_EVENT_TYPE));
        f4Client.addURIProperty(eventURI, RdfConstants.PREMIS_HAS_EVENT_TYPE,
                new URI("http://id.loc.gov/vocabulary/preservationEvents/ingestion"));
        f4Client.addDateProperty(eventURI, RdfConstants.PREMIS_HAS_EVENT_DATE, new Date());
        f4Client.addIntegerProperty(eventURI, RdfConstants.PRES_BAG_SIZE, bs.getFile().length());
        f4Client.addIntegerProperty(eventURI, RdfConstants.PRES_BAG_PAYLOAD_SIZE, bs.getBagPayloadSize());
        f4Client.addLiteralProperty(eventURI, RdfConstants.PRES_BAG_ID, ts.getEtag());
        final URI manifestURI = f4Client.createNonRDFResource(eventURI, bs.getManifestCopy(), "text/plain");
        f4Client.addURIProperty(eventURI, RdfConstants.PRES_HAS_BAG_MANIFEST, manifestURI);
        f4Client.addURIProperty(uri, RdfConstants.PREMIS_HAS_EVENT, eventURI);

        while (triplestore.getQueryResponse(
                "SELECT (COUNT(?s) AS ?count) WHERE { ?s <" + RdfConstants.PREMIS_HAS_EVENT + "> <" + eventURI + "> }")
                .get(0).get("count") == "0") {
            LOGGER.debug("Waiting for update to propagate to triplestore...");
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                // no worries, we just got woken up early...
            }
        }
    }

    private long getPayloadBytesSubmitted() throws IOException {
        return Long.parseLong(triplestore
                .getQueryResponse("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                        + "SELECT (SUM(?bytes) AS ?totalBytes)\n" + "WHERE {\n"
                        + "    ?s rdf:type <http://fedora.lib.virginia.edu/preservation#APTrustEvent> .\n"
                        + "    ?s <http://www.loc.gov/premis/rdf/v1#hasEventType> <http://id.loc.gov/vocabulary/preservationEvents/ingestion> .\n"
                        + "    ?s <http://fedora.lib.virginia.edu/preservation#bagPayloadSize> ?bytes\n" + "}")
                .get(0).get("totalBytes"));
    }

    private boolean hasBeenSent(String uri) throws IOException {
        return !triplestore
                .getQueryResponse("SELECT (COUNT(?event) AS ?count)\n" + "WHERE {\n" + "  <" + uri
                        + "> <http://www.loc.gov/premis/rdf/v1#hasEvent> ?event\n" + "}")
                .get(0).get("count").equals("0");
    }

    /**
     * This queries the triplestore and returns only items that are eligible for submission and
     * have not yet been submitted (even if they were submitted and failed).
     */
    private List<Map<String, String>> getNextPageOfWSLSResultsToSubmit(int pageSize) throws IOException {
        return triplestore.getQueryResponse("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + "SELECT DISTINCT ?s ?wslsid\n" + "WHERE {\n"
                + "    { {\n"
                + "      ?s rdf:type <http://fedora.lib.virginia.edu/preservation#PreservationPackage> .\n"
                + "      ?s <http://purl.org/dc/terms/rights> ?rights .\n" + "      ?s dc:identifier ?wslsid\n"
                + "    } UNION {\n"
                + "      ?s rdf:type <http://fedora.lib.virginia.edu/preservation#PreservationPackage> . \n"
                + "      ?title <http://fedora.lib.virginia.edu/preservation#hasFile> ?s .\n"
                + "      ?title <http://purl.org/dc/terms/rights> ?rights .\n" + "      ?title dc:identifier ?wslsid\n"
                + "    } }\n" + "    MINUS {\n" + "      ?s <http://www.loc.gov/premis/rdf/v1#hasEvent> ?event\n"
                + "    }\n" + "} \n" + "ORDER BY ?wslsid\n" + "LIMIT " + pageSize);
    }
}
