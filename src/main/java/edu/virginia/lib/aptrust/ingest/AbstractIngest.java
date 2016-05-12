package edu.virginia.lib.aptrust.ingest;

import com.hp.hpl.jena.rdf.model.Model;

import edu.virginia.lib.aptrust.RdfConstants;
import edu.virginia.lib.aptrust.helper.ExternalSystem;
import edu.virginia.lib.aptrust.helper.FederatedFile;
import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;
import edu.virginia.lib.aptrust.helper.HttpHelper;
import edu.virginia.lib.aptrust.helper.mediainfo.MediaInfoProcess;

import org.fcrepo.client.FcrepoOperationFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;

public abstract class AbstractIngest {
	
	final private static int MS_TO_WAIT = 150;

    final private static Logger LOGGER = LoggerFactory.getLogger(AbstractIngest.class);

    protected Fedora4Client f4Writer;

    protected FusekiReader triplestore;

    public AbstractIngest(Fedora4Client f4Writer, FusekiReader triplestore) {
        this.f4Writer = f4Writer;
        this.triplestore = triplestore;
    }
    
    /**
     * If all of the resources produced by a subclass of this class should be 
     * contained by a resource, that resource's path (relative to the root)
     * should be returned by this method.  Otherwise this should return null.
     * 
     * This is useful as an administrative container.  It will allow access
     * controls to be enforced at that level.
     */
    protected abstract String containerResource();

    /**
     * Finds or creates a resource representing an item in an external system.  If this method
     * creates a new resource, it waits until that new resource is findable within the RDF
     * triplestore before returning.
     */
    protected URI findOrCreateFedoraExternalResource(String externalSystemId, ExternalSystem externalSystem, boolean preservationPackage, boolean syncIndexUpdate) throws IOException, URISyntaxException, FcrepoOperationFailedException {
        URI id = lookupFedora4URIForExternalId(externalSystemId, externalSystem);
        if (id == null) {
            id = createResource(externalSystemId, externalSystem, preservationPackage, syncIndexUpdate);
            LOGGER.debug(externalSystemId + " Created new object " + id + ".");
        } else {
            LOGGER.debug(externalSystemId + " Found existing object " + id + ".");
        }
        return id;
    }

    /**
     * Finds a resource representing an external system with the given ID.  If no
     * such resource is found, this method returns null.
     */
    protected ExternalSystem findExternalSystem(String id) throws IOException, URISyntaxException, FcrepoOperationFailedException {
        URI uri = lookupFedora4URI(id, RdfConstants.EXTERNAL_SYSTEM_TYPE);
        if (uri == null) {
            return null;
        }
        Model m = f4Writer.getAllProperties(uri);
        return new ExternalSystem(Fedora4Client.getFirstPropertyValue(m, uri, RdfConstants.DC_IDENTIFIER),
                Fedora4Client.getFirstPropertyValue(m, uri, RdfConstants.DCTERMS_DESCRIPTION),
                uri);
    }

    /**
     * Creates a resource representing an external system with the given ID and
     * description.  If another resource exists with that ID, an exception is
     * thrown.
     */
    protected ExternalSystem createExternalSystem(String id, String description, boolean syncIndexUpdate) throws FcrepoOperationFailedException, URISyntaxException, IOException {
        final URI uri = f4Writer.createResource(containerResource());
        f4Writer.addLiteralProperty(uri, RdfConstants.DC_IDENTIFIER, id);
        f4Writer.addURIProperty(uri, RdfConstants.RDF_TYPE, new URI(RdfConstants.EXTERNAL_SYSTEM_TYPE));
        f4Writer.addLiteralProperty(uri, RdfConstants.DCTERMS_DESCRIPTION, description);
        while (syncIndexUpdate && lookupFedora4URI(id, RdfConstants.EXTERNAL_SYSTEM_TYPE) == null) {
            LOGGER.debug("Waiting for update to propagate to triplestore...");
            try {
                Thread.sleep(MS_TO_WAIT);
            } catch (InterruptedException e) {
                // no worries, we just got woken up early...
            }
        }
        return new ExternalSystem(id, description, uri);
    }

    /**
     * Finds or creates a resource representing a File.
     * @param syncIndexUpdate if true and this method creates a new resource, it waits until that
     *                        new resource is findable within the RDF triplestore before returning.
     */
    protected URI createOrLocateFileResource(FederatedFile ff, boolean preservationPackage, boolean syncIndexUpdate) throws URISyntaxException, IOException, FcrepoOperationFailedException, InterruptedException {
        URI id = lookupFedora4URI(ff.getURI(), RdfConstants.FILE_TYPE);
        if (id == null) {
            // create the object
            id = createFileResource(ff, preservationPackage, syncIndexUpdate);
            f4Writer.addLiteralProperty(id, RdfConstants.DC_TITLE, ff.getFile().getName());

            // create the master file resource
            final URI masterId = f4Writer.createRedirectNonRDFResource(new URI(ff.getURI()), id);
            final URI masterRDFURI = new URI(masterId.toString() + "/fcr:metadata");
            f4Writer.addLiteralProperty(masterRDFURI, RdfConstants.FILENAME, ff.getFile().getName());
            f4Writer.addLiteralProperty(masterRDFURI, RdfConstants.DC_TITLE, ff.getFile().getName());
            f4Writer.addLiteralProperty(masterRDFURI, RdfConstants.FILE_URI, ff.getFile().toURI().toString());
            f4Writer.addURIProperty(id, RdfConstants.HAS_BINARY, masterId);

            // create tech metadata resource
            File mediaInfo = File.createTempFile("media-info", ".technical-metadata.txt");
            if (ff.getFile().exists()) {
                new MediaInfoProcess().generateMediaInfoReport(ff.getFile(), mediaInfo);
            } else {
                LOGGER.warn("Downloading file " + ff.getFile().getName() + " to generate mediainfo report.");
                // download it to generate the MediaInfo Report
                File export = File.createTempFile("master-file-temp", ff.getFile().getName());
                FileOutputStream fos = new FileOutputStream(export);
                try {
                    HttpHelper.getContentAtURL(ff.getURI().toString(), fos);
                } finally {
                    fos.close();
                }
                new MediaInfoProcess().generateMediaInfoReport(export, mediaInfo);
            }
            final URI techMDId = f4Writer.createNonRDFResource(id, mediaInfo, "text/plain");
            final URI techMDURI = new URI(techMDId.toString() + "/fcr:metadata");
            f4Writer.addLiteralProperty(techMDURI, RdfConstants.FILENAME, RdfConstants.TECH_MD_FILENAME + ".txt");
            f4Writer.addLiteralProperty(techMDURI, RdfConstants.DC_TITLE, RdfConstants.TECH_MD_FILENAME + ".txt");
            f4Writer.addURIProperty(id, RdfConstants.HAS_TECH_MD, techMDId);


            while (syncIndexUpdate && lookupFedora4URI(ff.getURI(), RdfConstants.FILE_TYPE) == null) {
                LOGGER.debug("Waiting for file resource creation to propagate to triplestore...");
                try {
                    Thread.sleep(MS_TO_WAIT);
                } catch (InterruptedException e) {
                    // no worries, we just got woken up early...
                }
            }
        }
        return id;
    }

    /**
     * Finds or creates a resource representing a File.
     * @param syncIndexUpdate if true and this method creates a new resource, it waits until that
     *                        new resource is findable within the RDF triplestore before returning.
     */
    protected URI createOrLocateFileResource(File f, final String mimeType, boolean preservationPackage, boolean syncIndexUpdate) throws URISyntaxException, IOException, FcrepoOperationFailedException, InterruptedException {
        URI id = lookupFedora4URI(f.getName(), RdfConstants.FILE_TYPE);
        if (id == null) {
            // create the object
            id = createFileResource(f, preservationPackage, syncIndexUpdate);
            f4Writer.addLiteralProperty(id, RdfConstants.DC_TITLE, f.getName());

            // create the master file resource
            final URI masterId = f4Writer.createNonRDFResource(id, f, mimeType);
            final URI masterRDFURI = new URI(masterId.toString() + "/fcr:metadata");
            f4Writer.addLiteralProperty(masterRDFURI, RdfConstants.FILENAME, f.getName());
            f4Writer.addLiteralProperty(masterRDFURI, RdfConstants.DC_TITLE, f.getName());
            f4Writer.addURIProperty(id, RdfConstants.HAS_BINARY, masterId);

            while (syncIndexUpdate && lookupFedora4URI(f.getName(), RdfConstants.FILE_TYPE) == null) {
                LOGGER.debug("Waiting for file resource creation to propagate to triplestore...");
                try {
                    Thread.sleep(MS_TO_WAIT);
                } catch (InterruptedException e) {
                    // no worries, we just got woken up early...
                }
            }
        }
        return id;
    }

    /**
     * Creates a resource with the given identifier and type and then waits for the update to propagate to
     * the triplestore.
     */
    protected URI createResource(String id, URI rdfType, boolean preservationPackage, boolean syncIndexUpdate) throws FcrepoOperationFailedException, URISyntaxException, IOException {
        final URI uri = f4Writer.createResource(containerResource());
        f4Writer.addLiteralProperty(uri, RdfConstants.DC_IDENTIFIER, id);
        f4Writer.addURIProperty(uri, RdfConstants.RDF_TYPE, rdfType);
        if (preservationPackage) {
            f4Writer.addURIProperty(uri, RdfConstants.RDF_TYPE, new URI(RdfConstants.PRESERVATION_PACKAGE_TYPE));
        }
        while (syncIndexUpdate && lookupFedora4URI(id, rdfType.toString()) == null) {
            LOGGER.debug("Waiting for update to propagate to triplestore...");
            try {
                Thread.sleep(MS_TO_WAIT);
            } catch (InterruptedException e) {
                // no worries, we just got woken up early...
            }
        }
        return uri;
    }

    private URI createFileResource(FederatedFile ff, boolean preservationPackage, boolean syncIndexUpdate) throws FcrepoOperationFailedException, URISyntaxException, IOException {
        final URI uri = f4Writer.createResource(containerResource());
        f4Writer.addLiteralProperty(uri, RdfConstants.DC_IDENTIFIER, ff.getURI());
        f4Writer.addURIProperty(uri, RdfConstants.RDF_TYPE, new URI(RdfConstants.FILE_TYPE));
        if (preservationPackage) {
            f4Writer.addURIProperty(uri, RdfConstants.RDF_TYPE, new URI(RdfConstants.PRESERVATION_PACKAGE_TYPE));
        }
        while (syncIndexUpdate && lookupFedora4URI(ff.getURI(), RdfConstants.FILE_TYPE) == null) {
            LOGGER.debug("Waiting for update to propagate to triplestore...");
            try {
                Thread.sleep(MS_TO_WAIT);
            } catch (InterruptedException e) {
                // no worries, we just got woken up early...
            }
        }
        return uri;
    }

    private URI createFileResource(File f, boolean preservationPackage, boolean syncIndexUpdate) throws FcrepoOperationFailedException, URISyntaxException, IOException {
        final URI uri = f4Writer.createResource(containerResource());
        f4Writer.addLiteralProperty(uri, RdfConstants.DC_IDENTIFIER, f.getName());
        f4Writer.addURIProperty(uri, RdfConstants.RDF_TYPE, new URI(RdfConstants.FILE_TYPE));
        if (preservationPackage) {
            f4Writer.addURIProperty(uri, RdfConstants.RDF_TYPE, new URI(RdfConstants.PRESERVATION_PACKAGE_TYPE));
        }
        while (syncIndexUpdate && lookupFedora4URI(f.getName(), RdfConstants.FILE_TYPE) == null) {
            LOGGER.debug("Waiting for update to propagate to triplestore...");
            try {
                Thread.sleep(MS_TO_WAIT);
            } catch (InterruptedException e) {
                // no worries, we just got woken up early...
            }
        }
        return uri;
    }

    /**
     * Creates a new Resource in Fedora 4 representing an external resource and returns the id.
     * This method waits until the change has been propagated to the triplestore before returning.
     */
    private URI createResource(String externalSystemId, ExternalSystem externalSystem, boolean preservationPackage, boolean syncIndexUpdate) throws FcrepoOperationFailedException, URISyntaxException, IOException {
        final URI uri = f4Writer.createResource(containerResource());
        f4Writer.addLiteralProperty(uri, RdfConstants.EXTERNAL_ID, externalSystemId);
        f4Writer.addURIProperty(uri, RdfConstants.EXTERNAL_SYSTEM, externalSystem.getFedora4Uri());
        f4Writer.addURIProperty(uri, RdfConstants.RDF_TYPE, new URI(RdfConstants.EXTERNAL_RESOURCE_TYPE));
        if (preservationPackage) {
            f4Writer.addURIProperty(uri, RdfConstants.RDF_TYPE, new URI(RdfConstants.PRESERVATION_PACKAGE_TYPE));
        }
        while (syncIndexUpdate && lookupFedora4URIForExternalId(externalSystemId, externalSystem) == null) {
            LOGGER.debug("Waiting for update to propagate to triplestore...");
            try {
                Thread.sleep(MS_TO_WAIT);
            } catch (InterruptedException e) {
                // no worries, we just got woken up early...
            }
        }
        return uri;
    }

    /**
     * Gets the fedora 4 URI for the given id in the given external system if it has
     * been created.
     */
    private URI lookupFedora4URIForExternalId(String externalId, ExternalSystem externalSystem)
            throws IOException, URISyntaxException {
        String query = "PREFIX pres: <" + RdfConstants.UVA_PRESERVATION_NAMESPACE + ">\n" +
                "\n" +
                "SELECT ?s\n" +
                "WHERE {\n" +
                "  ?s pres:externalId '" + externalId + "' .\n" +
                "  ?s pres:externalSystem <" + externalSystem.getFedora4Uri()+ ">\n" +
                "}\n" +
                "LIMIT 2";
        LOGGER.trace(query);
        final String uriStr = triplestore.getFirstAndOnlyQueryResponse(query).get("s");
        if (uriStr == null) {
            return null;
        } else {
            return new URI(uriStr);
        }
    }

    /**
     * Gets the fedora 4 URI for a resource with the given dc:identifier and rdf:type.
     */
    protected URI lookupFedora4URI(String dcid, String rdfType)
            throws IOException, URISyntaxException {
    	/*
    	 * Apparently the camel route to fuseki or fuseki itself URLDecodes literal values sent to the
    	 * triplestore, such that queries must be decoded in order to find them.
    	 */
        String query =
                "SELECT ?s\n" +
                "WHERE {\n" +
                "  ?s <" + RdfConstants.DC_IDENTIFIER + "> '" + URLDecoder.decode(dcid, "UTF-8") + "' .\n" +
                "  ?s <" + RdfConstants.RDF_TYPE + "> <" + rdfType + "> \n" +
                "}\n" +
                "LIMIT 2";
        LOGGER.trace(query);
        final String uriStr = triplestore.getFirstAndOnlyQueryResponse(query).get("s");
        if (uriStr == null) {
            return null;
        } else {
            return new URI(uriStr);
        }
    }
}
