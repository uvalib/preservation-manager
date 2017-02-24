package edu.virginia.lib.aptrust.ingest;

import edu.virginia.lib.aptrust.RdfConstants;
import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getOptionalProperty;
import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

public class ASpaceBornDigitalIngest extends AbstractIngest {

    final private static Logger LOGGER = LoggerFactory.getLogger(ASpaceBornDigitalIngest.class);

    public static void main(String [] args) throws IOException, URISyntaxException, FcrepoOperationFailedException {
        final String accessionId = args[0];
        final String masterFile = args[1];
        final String ancillaryFileDir = args[2];

        Properties p = new Properties();
        FileInputStream fis = new FileInputStream("ingest.properties");
        try {
            p.load(fis);
        } finally {
            fis.close();
        }

        FusekiReader fuseki = new FusekiReader(getRequiredProperty(p, "triplestore-url"));
        Fedora4Client f4Client = new Fedora4Client(getOptionalProperty(p, "f4-username"), getOptionalProperty(p, "f4-password"), getRequiredProperty(p, "f4-url"));

        ASpaceBornDigitalIngest i = new ASpaceBornDigitalIngest(f4Client, fuseki);
        i.ingestDigitalFileAccession(accessionId, masterFile, new File(ancillaryFileDir));
    }

    private URI collectionUri;

    public ASpaceBornDigitalIngest(Fedora4Client f4Writer, FusekiReader triplestore) throws IOException, FcrepoOperationFailedException, URISyntaxException {
        super(f4Writer, triplestore);
        collectionUri = findOrCreateContainer(null);
    }

    public void ingestDigitalFileAccession(final String accessionId, final String filePath, final File packageDir) throws IOException, FcrepoOperationFailedException, URISyntaxException {
        final File ttlMetadata = new File(packageDir, accessionId + ".ttl");
        if (!ttlMetadata.exists()) {
            throw new RuntimeException("Unable to find metadata (" + ttlMetadata.getAbsolutePath() + ").");
        }

        // create or find accession container (as a convenience)
        final URI accessionUri = findOrCreateContainer(accessionId);

        final String existsQuery = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX pres4: <http://ontology.lib.virginia.edu/preservation#>\n" +
                "PREFIX fedora: <http://fedora.info/definitions/v4/repository#>\n" +
                "\n" +
                "SELECT ?uri\n" +
                "WHERE {\n" +
                "       ?uri fedora:hasParent <" + accessionUri + "> .\n" +
                "       ?uri pres4:hasFile ?file .\n" +
                "       ?file pres4:hasLocalPath '" + filePath + "'\n" +
                "}";
        if (triplestore.getFirstAndOnlyQueryResponse(existsQuery).isEmpty()) {
            // create the preservation package and file (same resource)
            final URI uri = f4Writer.createResourceWithTriples(String.valueOf(accessionUri), ttlMetadata);

            // add extra files
            for (File f : packageDir.listFiles()) {
                if (!f.equals(ttlMetadata)) {
                    final URI fileResourceId = f4Writer.createResource(String.valueOf(uri));
                    f4Writer.addURIProperty(fileResourceId, RdfConstants.RDF_TYPE, new URI(RdfConstants.FILE_TYPE));

                    final URI binaryResourceId = f4Writer.createNonRDFResource(uri, f, guessMimeType(f));
                    final URI binaryRDFURI = new URI(binaryResourceId.toString() + "/fcr:metadata");
                    f4Writer.addLiteralProperty(binaryRDFURI, RdfConstants.FILENAME, f.getName());
                    f4Writer.addLiteralProperty(binaryRDFURI, RdfConstants.DC_TITLE, f.getName());
                    f4Writer.addURIProperty(fileResourceId, RdfConstants.HAS_BINARY, binaryResourceId);
                    f4Writer.addURIProperty(uri, RdfConstants.HAS_FILE, fileResourceId);
                    if (isReadMe(f)) {
                        f4Writer.addURIProperty(uri, RdfConstants.PRES_HAS_README, fileResourceId);
                    }
                }
            }
            f4Writer.addURIProperty(uri, RdfConstants.RDF_TYPE, new URI(RdfConstants.PRESERVATION_PACKAGE_TYPE));

            // create the File
            final URI localFileResourceId = f4Writer.createResource(String.valueOf(uri));
            f4Writer.addURIProperty(localFileResourceId, RdfConstants.RDF_TYPE, new URI(RdfConstants.FILE_TYPE));
            f4Writer.addLiteralProperty(localFileResourceId, RdfConstants.HAS_LOCAL_PATH, filePath);
            f4Writer.addURIProperty(uri, RdfConstants.HAS_FILE, localFileResourceId);

            waitUntilTriplestoreIsUpdated(existsQuery);
        } else {
            LOGGER.info("This file is already in Fedora.");
        }

    }

    private boolean isReadMe(final File f) {
        return f.getName().toLowerCase().startsWith("readme");
    }

    private String guessMimeType(final File f) {
        if (f.getName().toLowerCase().endsWith(".txt")) {
            return "text/plain";
        } else if (f.getName().toLowerCase().endsWith(".info")) {
            return "text/plain";
        } else {
            throw new RuntimeException("NO MIME TYPE DETECED FOR " + f.getName() + "!");
            //return "application/octet-stream";
        }
    }

    /**
     * Waits until the query in the triplestore returns some value (as opposed to an empty result).
     */
    private void waitUntilTriplestoreIsUpdated(final String query) throws IOException {
        while (triplestore.getQueryResponse(query).isEmpty()) {
            LOGGER.debug("Waiting for file resource creation to propagate to triplestore...");
            try {
                Thread.sleep(MS_TO_WAIT);
            } catch (InterruptedException e) {
                // no worries, we just got woken up early...
            }
        }
    }

    @Override
    protected String containerResource() {
        return "aspace";
    }

    private URI findOrCreateContainer(final String childPath) throws FcrepoOperationFailedException, URISyntaxException, IOException {
        final String path = containerResource() + (childPath != null ? "/" + childPath : "");
        URI uri = new URI(f4Writer.getBaseUri().toString() + "/" + path);
        if (!f4Writer.exists(uri)) {
            // create collection resource
            uri = f4Writer.createNamedResource(path);
            if (!uri.equals(new URI(f4Writer.getBaseUri().toString() + "/" + path))) {
                throw new RuntimeException("URIs don't match " + uri + "!");
            }
        }
        return uri;
    }
}
