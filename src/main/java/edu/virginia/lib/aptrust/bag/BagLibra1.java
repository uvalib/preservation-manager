package edu.virginia.lib.aptrust.bag;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import edu.virginia.lib.aptrust.bags.APTrustInfo;
import edu.virginia.lib.aptrust.bags.BagInfo;
import edu.virginia.lib.aptrust.bags.BagSummary;
import edu.virginia.lib.aptrust.bags.util.BagSubmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;
import java.util.Properties;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getProperties;
import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

/**
 * Created by md5wz on 11/2/16.
 */
public class BagLibra1 {

    public static void main(String [] args) throws Exception {
        new BagLibra1(new File(args[0]));
    }

    final private static Logger LOGGER = LoggerFactory.getLogger(BagLibra1.class);

    private BagSubmitter submitter;

    public BagLibra1(final File dump) throws Exception {
        Properties p = getProperties("aws-credentials.properties");
        AWSCredentials credentials = new BasicAWSCredentials(getRequiredProperty(p, "accessKey"), getRequiredProperty(p, "secretKey"));
        AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);

        submitter = new BagSubmitter(amazonS3Client, getRequiredProperty(p, "bucketName"));

        // create bag
        Libra1ResubmissionBag bag = new Libra1ResubmissionBag(new BagInfo().sourceOrganization("virginia.edu"),
                new APTrustInfo("Libra: Online Archive of University of Virginia Scholarship", APTrustInfo.CONSORTIA), dump);
        LOGGER.debug("Creating and transferring bag for libra at " + new Date() + "...");
        BagSummary bs = bag.serializeAPTrustBag(new File("output"), true);
        LOGGER.debug(bs.getManifestCopy());
        LOGGER.info(bs.getFile().getName() + " created, " + bs.getFile().length()
                + " bytes with base64 checksum=" + bs.getBase64Checksum());

        // ingest bag
        BagSubmitter.TransferSummary ts = submitter.transferBag(bs, true);
        if (ts.wasTransferred()) {
            bs.getFile().delete();
            LOGGER.info("Transferred in " + ts.getDuration() + " ms.");
        } else {
            LOGGER.warn(bs.getFile() + " not transferred!  " + ts.getMessage());
            bs.getFile().delete();
        }
    }

}
