package edu.virginia.lib.aptrust;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getOptionalProperty;
import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getProperties;
import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import edu.virginia.lib.aptrust.bag.BagArchiveIt;
import edu.virginia.lib.aptrust.bag.Fedora4APTrustBag;
import edu.virginia.lib.aptrust.helper.Fedora4Client;

public class ProduceReadme {

    public static void main(String [] args) throws Exception {
        Properties ingestProperties = getProperties("ingest.properties");
        Fedora4Client f4Client = new Fedora4Client(getOptionalProperty(ingestProperties, "f4-username"), getOptionalProperty(ingestProperties, "f4-password"), getRequiredProperty(ingestProperties, "f4-url"));
        System.out.println(Fedora4APTrustBag.getReadmeForURI(new URI(args[0]), f4Client));
    }
}
