package edu.virginia.lib.aptrust.bag;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.hp.hpl.jena.rdf.model.Model;
import edu.virginia.lib.aptrust.RdfConstants;
import edu.virginia.lib.aptrust.bags.APTrustInfo;
import edu.virginia.lib.aptrust.bags.BagInfo;
import edu.virginia.lib.aptrust.bags.BagSummary;
import edu.virginia.lib.aptrust.bags.util.BagSubmitter;
import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Properties;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getOptionalProperty;
import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getProperties;
import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

public class BagResource {

    final private static Logger LOGGER = LoggerFactory.getLogger(BagResource.class);

    /** This directory must exist and must have space for 2x the bag size. **/
    private static final File WORKING_ROOT = new File("HUGE_WORKING_SPACE");

    public static void main(String [] args) throws IOException, URISyntaxException, Exception {
        new BagResource().makeAndSubmitBag(new URI(args[0]));
    }

    private Fedora4Client f4Client;

    private FusekiReader triplestore;

    private BagSubmitter submitter;

    public BagResource() throws Exception {
        Properties ingestProperties = getProperties("ingest.properties");

        f4Client = new Fedora4Client(getOptionalProperty(ingestProperties, "f4-username"), getOptionalProperty(ingestProperties, "f4-password"), getRequiredProperty(ingestProperties, "f4-url"));
        
        triplestore = new FusekiReader(getRequiredProperty(ingestProperties, "triplestore-url"));

        Properties p = getProperties("aws-credentials.properties");
        AWSCredentials credentials = new BasicAWSCredentials(getRequiredProperty(p, "accessKey"), getRequiredProperty(p, "secretKey"));
        AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);

        submitter = new BagSubmitter(amazonS3Client, getRequiredProperty(p, "bucketName"));
    }

    public void createLocalBagDirs(final URI uri) throws Exception {
        Model m = f4Client.getAllProperties(uri);

        LOGGER.info("Bagging " + uri + "...");
        Fedora4APTrustBag bag = new Fedora4APTrustBag(new BagInfo().sourceOrganization("virginia.edu"),
                    new APTrustInfo(Fedora4Client.getFirstPropertyValue(m, uri, RdfConstants.DC_TITLE), APTrustInfo.CONSORTIA), uri, f4Client, triplestore, WORKING_ROOT);
        LOGGER.debug("Creating and transferring bag for " + uri + " at " + new Date() + "...");
        BagSummary bs = bag.serializeAPTrustBag(new File(WORKING_ROOT, "output"), false);
        LOGGER.debug(bs.getManifestCopy());
        LOGGER.info(bs.getFile().getName() + " created, " + bs.getFile().length()
                + " bytes with base64 checksum=" + bs.getBase64Checksum());
     }

    public void makeAndSubmitBag(final URI uri) throws Exception {
        Model m = f4Client.getAllProperties(uri);

        LOGGER.info("Bagging " + uri + "...");
        Fedora4APTrustBag bag = new Fedora4APTrustBag(new BagInfo().sourceOrganization("virginia.edu"),
                new APTrustInfo(Fedora4Client.getFirstPropertyValue(m, uri, RdfConstants.DC_TITLE), APTrustInfo.CONSORTIA), uri, f4Client, triplestore, WORKING_ROOT);
        LOGGER.debug("Creating and transferring bag for " + uri + " at " + new Date() + "...");
        BagSummary bs = bag.serializeAPTrustBag(new File(WORKING_ROOT, "output"), true);
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
