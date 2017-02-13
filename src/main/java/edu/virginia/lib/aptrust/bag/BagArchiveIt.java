package edu.virginia.lib.aptrust.bag;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getOptionalProperty;
import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getProperties;
import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import edu.virginia.lib.aptrust.bags.APTrustInfo;
import edu.virginia.lib.aptrust.bags.BagInfo;
import edu.virginia.lib.aptrust.bags.BagSummary;
import edu.virginia.lib.aptrust.bags.util.BagSubmitter;
import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;

public class BagArchiveIt {

    final private static Logger LOGGER = LoggerFactory.getLogger(BagArchiveIt.class);
    
    public static void main(String [] args) throws IOException, URISyntaxException, Exception {
       new BagArchiveIt().ingestNewBags();
    }
    
    private Fedora4Client f4Client;

    private FusekiReader triplestore;

    private BagSubmitter submitter;
    
    public BagArchiveIt() throws IOException, URISyntaxException {
        Properties ingestProperties = getProperties("ingest.properties");

        f4Client = new Fedora4Client(getOptionalProperty(ingestProperties, "f4-username"), getOptionalProperty(ingestProperties, "f4-password"), getRequiredProperty(ingestProperties, "f4-url"));
        
        triplestore = new FusekiReader(getRequiredProperty(ingestProperties, "triplestore-url"));

        Properties p = getProperties("aws-credentials.properties");
        AWSCredentials credentials = new BasicAWSCredentials(getRequiredProperty(p, "accessKey"), getRequiredProperty(p, "secretKey"));
        AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);

        submitter = new BagSubmitter(amazonS3Client, getRequiredProperty(p, "bucketName"));
        
    }
    
    /**
     * Ingests the bag with the restoration plan for this system (ArchiveIt).
     */
    private void ingestSystemBag() throws Exception {
        throw new UnsupportedOperationException();
    }
    
    private void ingestNewBags() throws Exception {
        /*
        final String toIngestQuery = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + 
                "\n" + 
                "SELECT ?crawl ?collectionTitle ?crawlTitle\n" + 
                "WHERE {\n" + 
                "       ?crawl rdf:type <http://fedora.lib.virginia.edu/preservation#ArchiveItCrawl> .\n" + 
                "       ?crawl rdf:type <http://fedora.lib.virginia.edu/preservation#PreservationPackage> .\n" + 
                "       ?crawl <http://fedora.info/definitions/v4/repository#hasParent> ?p .\n" + 
                "       ?p dc:title ?collectionTitle .\n" + 
                "       ?crawl dc:identifier ?crawlTitle\n" + 
                "}";
                */
        final String toIngestQuery = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + 
                "PREFIX premis: <http://www.loc.gov/premis/rdf/v1#>\n" + 
                "\n" + 
                "SELECT ?crawl ?collectionTitle ?crawlTitle\n" + 
                "WHERE {\n" + 
                "       ?crawl rdf:type <http://fedora.lib.virginia.edu/preservation#ArchiveItCrawl> .\n" + 
                "       ?crawl rdf:type <http://fedora.lib.virginia.edu/preservation#PreservationPackage> .\n" + 
                "       ?crawl <http://fedora.info/definitions/v4/repository#hasParent> ?p .\n" + 
                "       ?p dc:title ?collectionTitle .\n" + 
                "       ?crawl dc:identifier ?crawlTitle\n" + 
                "    minus {\n" + 
                "     ?crawl premis:hasEvent ?event\n" + 
                "  }\n" + 
                "}";
        LOGGER.info("Querying for ArchiveIt crawls...");
        LOGGER.info(toIngestQuery);
        for (Map<String, String> m : triplestore.getQueryResponse(toIngestQuery)) {
            final URI uri = new URI(m.get("crawl"));
            LOGGER.info("Bagging " + uri + "...");
            Fedora4APTrustBag bag = new Fedora4APTrustBag(new BagInfo().sourceOrganization("virginia.edu"),
                    new APTrustInfo(m.get("crawlTitle") + " crawl of \"" + m.get("collectionTitle") + "\"", APTrustInfo.CONSORTIA), uri, f4Client, triplestore);
            LOGGER.debug("Creating and transferring bag for " + uri + " at " + new Date() + "...");
            BagSummary bs = bag.serializeAPTrustBag(new File("output"), true);
            LOGGER.debug(bs.getManifestCopy());
            LOGGER.info(bs.getFile().getName() + " created, " + bs.getFile().length()
                    + " bytes with base64 checksum=" + bs.getBase64Checksum());
            
            // ingest bag
            BagSubmitter.TransferSummary ts = submitter.transferBag(bs, false);
            if (ts.wasTransferred()) {
                Bagger.createPremisEventForIngest(f4Client, triplestore, uri, bs, ts);
                bs.getFile().delete();
                LOGGER.info("Transferred in " + ts.getDuration() + " ms.");
            } else {
                LOGGER.warn(bs.getFile() + " not transferred!  " + ts.getMessage());
                bs.getFile().delete();
            }

        }
        
    }
    
    public void createLocalBagDirs() throws Exception {
        final String toIngestQuery = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + 
                "\n" + 
                "SELECT ?crawl ?collectionTitle ?crawlTitle\n" + 
                "WHERE {\n" + 
                "       ?crawl rdf:type <http://fedora.lib.virginia.edu/preservation#ArchiveItCrawl> .\n" + 
                "       ?crawl rdf:type <http://fedora.lib.virginia.edu/preservation#PreservationPackage> .\n" + 
                "       ?crawl <http://fedora.info/definitions/v4/repository#hasParent> ?p .\n" + 
                "       ?p dc:title ?collectionTitle .\n" + 
                "       ?crawl dc:identifier ?crawlTitle\n" + 
                "}";
        LOGGER.info("Querying for ArchiveIt crawls...");
        for (Map<String, String> m : triplestore.getQueryResponse(toIngestQuery)) {
            final URI uri = new URI(m.get("crawl"));
            LOGGER.info("Bagging " + uri + "...");
            Fedora4APTrustBag bag = new Fedora4APTrustBag(new BagInfo().sourceOrganization("virginia.edu"),
                    new APTrustInfo(m.get("crawlTitle") + " crawl of \"" + m.get("collectionTitle") + "\"", APTrustInfo.CONSORTIA), uri, f4Client, triplestore);
            LOGGER.debug("Creating and transferring bag for " + uri + " at " + new Date() + "...");
            BagSummary bs = bag.serializeAPTrustBag(new File("output"), false);
            LOGGER.debug(bs.getManifestCopy());
            LOGGER.info(bs.getFile().getName() + " created, " + bs.getFile().length()
                    + " bytes with base64 checksum=" + bs.getBase64Checksum());
        }
    }
    
}
