package edu.virginia.lib.aptrust.bag;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;

import edu.virginia.lib.aptrust.RdfConstants;
import edu.virginia.lib.aptrust.bags.APTrustBag;
import edu.virginia.lib.aptrust.bags.APTrustInfo;
import edu.virginia.lib.aptrust.bags.BagInfo;
import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;
import edu.virginia.lib.aptrust.helper.HttpHelper;

/**
 * Extends the basic APTrustBag to support bagging Fedora 4 resources produced using the AvalonIngest code.
 */
public class Fedora4APTrustBag extends APTrustBag {

    private static final String FEDORA4_JCR_EXPORT = "exported-fedora-4-resource.xml";
    private static final String FEDORA3_EXPORT = "exported-fedora-3-resource.xml";

    // there's no easy way to just import import static org.fcrepo.kernel.api.RdfLexicon.HAS_MIME_TYPE;
    final static String HAS_MIME_TYPE = "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#hasMimeType";

    final private static Logger LOGGER = LoggerFactory.getLogger(Fedora4APTrustBag.class);

    private URI uri;

    private List<File> tempFiles;
    private List<File> payloadFiles;

    private Fedora4Client f4client;

    private FusekiReader triplestore;

    private File workingDir;

    public Fedora4APTrustBag(BagInfo bagInfo, APTrustInfo aptrustInfo, URI fedora4uri, Fedora4Client f4client, FusekiReader triplestore) {
        super(bagInfo, aptrustInfo);
        this.uri = fedora4uri;
        this.f4client = f4client;
        this.triplestore = triplestore;
        workingDir = new File("work");
        workingDir.mkdir();
    }

    @Override
    protected String getItemId() {
        return uri.getPath().substring(uri.getPath().lastIndexOf('/') + 1);
    }

    /**
     * The payload files include
     * 1.  a fedora 4 export of the resource (data/exported-fedora-4-resource.xml)
     * 2.  a fedora 3 export if the resource represented a single managed fedora 3 resource (data/exported-fedora-4-resource.xml)
     * 3.  all immediately contained binary files (data/filename)
     * 4.  all immediately contained external resources (data/pid.xml)   
     */
    @Override
    protected List<File> getPayloadFiles() throws Exception {
        if (payloadFiles != null) {
            return payloadFiles;
        }
        payloadFiles = new ArrayList<File>();
        tempFiles = new ArrayList<File>();

        // export resource to a temp file
        // currently there's no valid export on fedora 4.7
        //File export = exportF4ResouceToTempFile(uri.toString(), FEDORA4_JCR_EXPORT);
        //payloadFiles.add(export);
        //tempFiles.add(export);

        Model m = f4client.getAllProperties(uri);

        if (Fedora4Client.hasType(m, uri.toString(), RdfConstants.EXTERNAL_RESOURCE_TYPE)) {
            /* Export External Content
             *
             * Right now we assume any external content is fedora 3 content... when this
             * assumption is no longer true, we'll add more information into the repository
             * and improve this bagging code...
             */
            final String externalId = Fedora4Client.getFirstPropertyValue(m, uri, RdfConstants.EXTERNAL_ID);
            final String externalSystemId = Fedora4Client.getFirstPropertyValue(m, uri, RdfConstants.EXTERNAL_SYSTEM);
            final File file = downloadURIToTempFile(triplestore.getFirstAndOnlyQueryResponse("SELECT ?t WHERE { <" + externalSystemId.toString() + "> <" + RdfConstants.DC_IDENTIFIER + "> ?t }").get("t") + "/objects/" + externalId + "/export?context=archive", FEDORA3_EXPORT);
            payloadFiles.add(file);
            tempFiles.add(file);
        }
        
        // for ArchiveIt crawls, include the seed information from the collection
        if (Fedora4Client.hasType(m, uri.toString(), RdfConstants.ARCHIVE_IT_CRAWL_TYPE)) {
            final String seedListUrl = triplestore.getFirstAndOnlyQueryResponse("PREFIX pres: <http://fedora.lib.virginia.edu/preservation#>\n" + 
                    "\n" + 
                    "SELECT ?seedlist\n" + 
                    "WHERE {\n" + 
                    "  <http://fedora4test.lib.virginia.edu:8080/fcrepo/rest/archiveIt/77/80/4e/85/77804e85-39b1-47a2-a3e4-77da384d6a8d/d8/e8/0c/a9/d8e80ca9-aa02-40b7-8e3e-4253756ccb73> <http://fedora.info/definitions/v4/repository#hasParent> ?collection .\n" + 
                    "  ?collection pres:hasWarcSeedList ?seedlist .\n" + 
                    "}").get("seedlist");
            final File file = getNamedTempFile("collection-seed-list.csv");
            FileOutputStream fos = new FileOutputStream(file);
            try {
                f4client.download(URI.create(seedListUrl), fos);
            } finally {
                fos.close();
            }
            payloadFiles.add(file);
            tempFiles.add(file);
        }


        // locate or export any contained binaries or ExternalResource's
        URI nestedExternalSystemResourceURI = null;
        for (RDFNode n : f4client.getPropertyValues(uri, uri, RdfConstants.LDP_CONTAINS)) {
            final URI containedUri = new URI(n.asResource().getURI());
            final URI metadataUri = new URI(containedUri.toString() + "/fcr:metadata");
            Model containedM = f4client.getAllProperties(metadataUri);
            if (Fedora4Client.hasType(containedM, containedUri.toString(), RdfConstants.FEDORA_BINARY)) {
                final String mimeType = f4client.getSingleRequiredPropertyValue(metadataUri, new URI(containedUri.toString()), HAS_MIME_TYPE);
                if (mimeType.startsWith("message/external-body")) {
                    // this is a convention we use to point to an external file... the path of that file is stored elsewhere...
                    final String fileURI = f4client.getSingleRequiredPropertyValue(metadataUri, new URI(containedUri.toString()), RdfConstants.FILE_URI);
                    File file = new File(new URI(fileURI));
                    if (file.exists()) {
                        payloadFiles.add(file);
                    } else {
                        LOGGER.warn("Unable to locate file " + file.getAbsolutePath() + ", downloading copy!");
                        file = downloadURIToTempFile(containedUri.toString(), file.getName());
                        payloadFiles.add(file);
                        tempFiles.add(file);
                    }
                } else {
                    final File file = downloadURIToTempFile(containedUri.toString(), f4client.getSingleRequiredPropertyValue(metadataUri, new URI(containedUri.toString()), RdfConstants.FILENAME));
                    payloadFiles.add(file);
                    tempFiles.add(file);
                }
            } else if (Fedora4Client.hasType(containedM,  containedUri.toString(), RdfConstants.EXTERNAL_RESOURCE_TYPE)) {
                final String externalId = Fedora4Client.getFirstPropertyValue(containedM, containedUri, RdfConstants.EXTERNAL_ID);
                final String externalSystemId = Fedora4Client.getFirstPropertyValue(containedM, containedUri, RdfConstants.EXTERNAL_SYSTEM);
                final String dcIdentifier = Fedora4Client.getFirstPropertyValue(containedM, containedUri, RdfConstants.DC_IDENTIFIER);
                if (nestedExternalSystemResourceURI == null) {
                    nestedExternalSystemResourceURI = new URI(externalSystemId);
                } else if (!nestedExternalSystemResourceURI.equals(new URI(externalSystemId))) {
                    throw new RuntimeException("Nested system mismatch for " + containedUri + ": " + externalSystemId + " != " + nestedExternalSystemResourceURI);
                }
                if (!externalId.startsWith("http")) {
                    final File file = downloadURIToTempFile(triplestore.getFirstAndOnlyQueryResponse("SELECT ?t WHERE { <" + externalSystemId.toString() + "> <" + RdfConstants.DC_IDENTIFIER + "> ?t }").get("t") + "/objects/" + externalId + "/export?context=archive", externalId + ".xml");
                    payloadFiles.add(file);
                    tempFiles.add(file);
                } else {
                    final File file = downloadURIToTempFile(externalId, dcIdentifier);
                    payloadFiles.add(file);
                    tempFiles.add(file);
                }
            } else {
                LOGGER.info("Skipping contained resource " + containedUri + " because it wasn't binary.");
            }
        }
        
        addReadme(m, nestedExternalSystemResourceURI);

        return payloadFiles;
    }

    private void addReadme(Model rdfProperties, URI nestedExternalSystemResourceURI) throws IOException, FcrepoOperationFailedException, URISyntaxException {
        File readmeFile = new File(workingDir, "readme.txt");
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(readmeFile)));
        try {
            // Include
            // 1. server hosting the canonical object (fedora 4 staging repository)
            pw.println(uri);
            pw.println("This is a package from the University of Virginia meant for long-term " +
                       "preservation in AP Trust.");
            pw.println("http://academicpreservationtrust.org/");
            pw.println();

            // 2. rights information as best known
            pw.println(getRightsStatement(rdfProperties));
            
            // 3. info about original system
            if (Fedora4Client.hasType(rdfProperties, uri.toString(), RdfConstants.EXTERNAL_RESOURCE_TYPE)) {
                final URI externalSystemURI = new URI(Fedora4Client.getFirstPropertyValue(rdfProperties, uri, RdfConstants.EXTERNAL_SYSTEM));
                Model externalSystemRdf = f4client.getAllProperties(externalSystemURI);
                final String note = Fedora4Client.getFirstPropertyValue(externalSystemRdf,  nestedExternalSystemResourceURI,  RdfConstants.SKOS_NAMESPACE + "note");
                if (note == null) {
                    pw.println(FEDORA3_EXPORT + " is exported from " + Fedora4Client.getFirstPropertyValue(externalSystemRdf, externalSystemURI, RdfConstants.DCTERMS_DESCRIPTION));
                    pw.println("Service URL: " + Fedora4Client.getFirstPropertyValue(externalSystemRdf, externalSystemURI, RdfConstants.DC_IDENTIFIER));
                    pw.println("PID: " + Fedora4Client.getFirstPropertyValue(rdfProperties, uri, RdfConstants.EXTERNAL_ID));
                    pw.println();
                } else {
                    pw.println(note);
                    pw.println();
                }
            }
            if (nestedExternalSystemResourceURI != null) {
                Model externalSystemRdf = f4client.getAllProperties(nestedExternalSystemResourceURI);
                final String note = Fedora4Client.getFirstPropertyValue(externalSystemRdf,  nestedExternalSystemResourceURI,  RdfConstants.SKOS_NAMESPACE + "note");
                if (note != null) {
                    pw.println("\nThe data files were exported from " + Fedora4Client.getFirstPropertyValue(externalSystemRdf, nestedExternalSystemResourceURI, RdfConstants.DCTERMS_DESCRIPTION) + ".");
                    pw.println(note);                    
                    if (Fedora4Client.hasType(externalSystemRdf, nestedExternalSystemResourceURI.toString(), RdfConstants.PRESERVATION_PACKAGE_TYPE)) {
                        pw.println("For more information and a restoration guide, fetch the bag " + bagNameFromURI(nestedExternalSystemResourceURI));
                    }
                    pw.println();

                } else {
                    pw.println("The xml files were exported from " + Fedora4Client.getFirstPropertyValue(externalSystemRdf, nestedExternalSystemResourceURI, RdfConstants.DCTERMS_DESCRIPTION));
                    pw.println("Service URL: " + Fedora4Client.getFirstPropertyValue(externalSystemRdf, nestedExternalSystemResourceURI, RdfConstants.DC_IDENTIFIER));
                    pw.println("The individual pids can be gleaned from the filenames which have the pattern {pid}.xml");
                    pw.println();
                }
            }

        } finally {
            pw.close();
        }
        payloadFiles.add(readmeFile);
        tempFiles.add(readmeFile);
    }
    
    public static final String bagNameFromURI(final URI uri) {
        final String str = uri.toString();
        return "virginia.edu." + str.substring(str.lastIndexOf('/') + 1) + ".tar";
    }
    
    /**
     * Gets rights information for the resource described by the given RDF properties.  This 
     * method uses several techniques to try to identify the rights statement that applies.
     * 
     * If none can be found, it returns "Rights Unknown".
     */  
    private String getRightsStatement(Model rdfProperties) throws IOException, URISyntaxException {
        StringBuffer sb = new StringBuffer();
        final boolean isFile = Fedora4Client.hasType(rdfProperties, uri.toString(), RdfConstants.FILE_TYPE);
        List<Map<String, String>> vars = null;
        if (isFile) {
            vars = triplestore.getQueryResponse("SELECT distinct ?r ?rt ?rd\n" +
                    "WHERE {\n" +
                    "  ?owner <http://fedora.lib.virginia.edu/preservation#hasFile> <" + uri + "> .\n" +
                    "  ?owner  <http://purl.org/dc/terms/rights> ?r .\n" +
                    "  ?r <http://purl.org/dc/elements/1.1/title> ?rt .\n" +
                    "  ?r <http://www.w3.org/2004/02/skos/core#definition> ?rd\n" +
                    "}");
        } else {
            vars = triplestore.getQueryResponse("SELECT ?r ?rt ?rd\n" +
                    "WHERE {\n" +
                    "  <" + uri + "> <http://purl.org/dc/terms/rights> ?r .\n" +
                    "  ?r <http://purl.org/dc/elements/1.1/title> ?rt .\n" +
                    "  ?r <http://www.w3.org/2004/02/skos/core#definition> ?rd\n" +
                    "}");
        }
        if (!vars.isEmpty()) {
            sb.append("Rights Statement:\n");
            for (Map<String, String> rightsStatement : vars) {
                sb.append(rightsStatement.get("rt") + "\n");
                sb.append(rightsStatement.get("r") + "\n");
                sb.append(rightsStatement.get("rd") + "\n");
                sb.append("\n");
            }
        } else {
            vars = triplestore.getQueryResponse("SELECT ?r \n" +
                    "WHERE {\n" +
                    "  <" + uri + "> <http://purl.org/dc/terms/rights> ?r .\n" +
                    "}");
            if (!vars.isEmpty()) {
                sb.append("Rights Statement:\n");
                for (Map<String, String> rightsStatement : vars) {
                    final String r = rightsStatement.get("r");
                    try {
                        sb.append(summarizeLinkedRightsStatement(new URI(r)) + "\n");
                    } catch (URISyntaxException ex) {
                        sb.append(r + "\n");
                    }
                }    
            } else {
                sb.append("Rights Unknown");
            }
        }
        return sb.toString();
    }

    /**
     * This should resolve or look up an external rights statement and provide
     * a concise human readable summary.
     */
    private static final String summarizeLinkedRightsStatement(final URI uri) {
        if (uri.toString().equals("http://rightsstatements.org/vocab/CNE/1.0/")) {
            return "Copyright Not Evaluated (" + uri.toString() + ")";
        } else {
            return uri.toString();
        }
        
    }

    private File getNamedTempFile(String filename) {
        File export = new File(workingDir, filename);
        export.getParentFile().mkdirs();
        if (export.exists()) {
            throw new RuntimeException("File already exists! " + export.getAbsolutePath());
        }
        return export;
    }
    
    private File downloadURIToTempFile(String url, String filename) throws IOException, URISyntaxException {
        File export = null;
        if (filename == null) {
            final URI uri = new URI(url);
            export = getNamedTempFile(uri.getHost() + "-" + uri.getPort() + "-" + uri.getPath());
        } else {
            export = getNamedTempFile(filename);
        }
        if (export.exists()) {
            throw new RuntimeException("File already exists! " + export.getAbsolutePath());
        }
        FileOutputStream fos = new FileOutputStream(export);
        try {
            HttpHelper.getContentAtURL(url, fos);
        } finally {
            fos.close();
        }
        return export;
    }

    private File exportF4ResouceToTempFile(String url, String filename) throws IOException, URISyntaxException, FcrepoOperationFailedException {
        File export = new File(workingDir, filename != null ? filename : URLEncoder.encode(url, "UTF-8"));
        FileOutputStream fos = new FileOutputStream(export);
        try {
            f4client.export(uri, fos, true, true);
        } finally {
            fos.close();
        }
        return export;
    }

    @Override
    protected void freePayloadFile(File file) throws Exception {
        for (File f : this.tempFiles) {
            LOGGER.debug("Deleting " + f.getAbsolutePath() + "");
            f.delete();
        }
    }

    @Override
    protected String getInstitutionalId() {
        return "virginia.edu";
    }

}
