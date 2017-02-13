package edu.virginia.lib.aptrust.ingest;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getOptionalProperty;
import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.fcrepo.client.FcrepoOperationFailedException;
import org.xml.sax.SAXException;

import com.yourmediashelf.fedora.client.FedoraClientException;

import edu.virginia.lib.aptrust.RdfConstants;
import edu.virginia.lib.aptrust.helper.ArchiveItClient;
import edu.virginia.lib.aptrust.helper.ArchiveItClient.Crawl;
import edu.virginia.lib.aptrust.helper.ArchiveItClient.Warc;
import edu.virginia.lib.aptrust.helper.ExternalSystem;
import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;

/**
 * ArchiveIt content is represented in Fedora in a way that mirrors the high level
 * organization of the preservation files.  Specifically we have constructs for the
 * Collections, Crawls and Warcs.  Each collection may have multiple craws.  Each 
 * crawl may have multiple warc files.
 *
 * We don't actually retain the WARC files in fedora, but instead push them to AP 
 * trust exactly once.
 */
public class ArchiveItIngest extends AbstractIngest implements RdfConstants {
    
    private static final String INC_EDU = "http://rightsstatements.org/vocab/InC-EDU/1.0/";
    
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
        
        ArchiveItClient ac = new ArchiveItClient(getRequiredProperty(p, "archive-it-username"), getRequiredProperty(p, "archive-it-password"));
        
        ArchiveItIngest ai = new ArchiveItIngest(f4Client, fuseki, ac);
        
        ai.findOrCreateArchiveItCollection("5422", "UVA Rolling Stone Article November 19, 2014 and Its Aftermath", new File("5422-collection-seed-list.csv"));
        ai.findOrCreateArchiveItCollection("6341", "UVA Rollingstones Aftermath (Part II)", new File("6341-collection-seed-list.csv"));
        ai.findOrCreateArchiveItCollection("7512", "University Registrar", new File("7512-collection-seed-list.csv"));
        ai.updateArchiveItCollections();
    }
    
    private URI collectionUri;
    
    private ArchiveItClient archiveItClient;
    
    private ExternalSystem archiveItSystem;
    
    public ArchiveItIngest(Fedora4Client f4Writer, FusekiReader triplestore, ArchiveItClient archiveIt) throws IOException, FcrepoOperationFailedException, URISyntaxException, FedoraClientException {
        super(f4Writer, triplestore);
        archiveItClient = archiveIt;
        collectionUri = findOrCreateArchiveItContainer();
        archiveItSystem = findOrCreateArchiveItSystemResource(); 
    }

    /**
     * Walks through all the existing ArchiveIt collections in Fedora and updates 
     * their contained crawl and warc references based on values retrieved from the
     * ArchiveIt API. 
     */
    public void updateArchiveItCollections() throws IOException, IllegalStateException, XPathExpressionException, ParserConfigurationException, SAXException, FcrepoOperationFailedException, URISyntaxException, InterruptedException {
        // find the collections
        final String findCollectionsQuery = "PREFIX pres: <http://fedora.lib.virginia.edu/preservation#>\n" + 
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + 
                " \n" + 
                "SELECT ?collection ?id\n" + 
                "WHERE {\n" + 
                "  ?collection rdf:type <http://fedora.lib.virginia.edu/preservation#ArchiveItCollection> .\n" + 
                "  ?collection dc:identifier ?id\n" + 
                "}\n";
        
        for (Map<String, String> m : triplestore.getQueryResponse(findCollectionsQuery)) {
            final String collectionUri = m.get("collection");
            final String archiveItCollectionId = m.get("id");
            
            // A map containing a key for each crawl title in Fedora whose values is a collection of the warc filenames 
            // from that crawl within fedora.
            Map<String, Collection<String>> crawlTitleToWarcFilenameMap = new HashMap<String, Collection<String>>();
            // A map from warc filenames in fedora to the recorded MD5 checksums.
            Map<String, String> warcFilenameToMD5Map = new HashMap<String, String>();
            
            final String findCrawlWarcsQuery = "PREFIX pres: <http://fedora.lib.virginia.edu/preservation#>\n" + 
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
                    "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + 
                    "PREFIX hash: <http://id.loc.gov/vocabulary/preservation/cryptographicHashFunctions/>\n" + 
                    "\n" + 
                    "SELECT ?crawl ?filename ?md5\n" + 
                    "WHERE {\n" + 
                    "       ?r <http://fedora.info/definitions/v4/repository#hasParent> <" + collectionUri +"> .\n" + 
                    "       ?r dc:identifier ?crawl .\n" + 
                    "       ?warc <http://fedora.info/definitions/v4/repository#hasParent> ?r .\n" + 
                    "       ?warc dc:identifier ?filename .\n" + 
                    "       ?warc hash:md5 ?md5 \n" + 
                    "}\n" + 
                    "";
            for (Map<String, String> entry : triplestore.getQueryResponse(findCrawlWarcsQuery)) {
                final String crawl = entry.get("crawl");
                Collection<String> warcs = crawlTitleToWarcFilenameMap.get(crawl);
                if (warcs == null) {
                    warcs = new ArrayList<String>();
                    crawlTitleToWarcFilenameMap.put(crawl, warcs);
                }
                warcs.add(entry.get("filename"));
                warcFilenameToMD5Map.put(entry.get("filename"), entry.get("md5"));
            }
            
            final Set<String> crawlChecklist = new HashSet<String>(crawlTitleToWarcFilenameMap.keySet());
            
            // for each crawl/warc in archiveIt, compare with fedora
            for (Crawl c : archiveItClient.getCrawls(archiveItCollectionId)) {
                System.out.println("Processing crawl \"" + c.getLabel() + "\"");
                crawlChecklist.remove(c.getLabel());
                if (crawlTitleToWarcFilenameMap.containsKey(c.getLabel())) {
                    // make sure the crawl is the same in fedora as in the API
                    final Set<String> warcChecklist = new HashSet<String>(crawlTitleToWarcFilenameMap.get(c.getLabel()));
                    for (Warc w : c.getWarcs()) {
                        if (!warcChecklist.remove(w.getFilename())) {
                            throw new RuntimeException("Warc with filename " + w.getFilename() + " is not in Fedora for the crawl " + c.getLabel() + "!");
                        } else if (!warcFilenameToMD5Map.get(w.getFilename()).equals(w.getMD5())) {
                            throw new RuntimeException("Warc with filename " + w.getFilename() + " has a different MD5 in fedora (" + warcFilenameToMD5Map.get(w.getFilename()) + ") than in ArchiveIt (" + w.getMD5() + ")!");
                        } else {
                            System.out.println("Warc with filename " + w.getFilename() + " is already known to fedora with a checksum of " + w.getMD5() + ".");
                        }
                    }
                    if (!warcChecklist.isEmpty()) {
                        throw new RuntimeException(warcChecklist.size() + " warc files exist in fedora for crawl " + c.getLabel() + " but aren't referenced by ArchiveIt!");
                    }
                } else {
                    // put this crawl information into Fedora
                    final URI crawlUri = createOrLocateTypedResource(collectionUri, c.getLabel(), new URI(ARCHIVE_IT_CRAWL_TYPE), true, true, new ResourceInitializer() {

                        @Override
                        public void initializeResource(URI uri) throws UnsupportedEncodingException, URISyntaxException,
                                FcrepoOperationFailedException, IOException {
                            f4Writer.addURIProperty(uri, RdfConstants.RIGHTS, new URI(INC_EDU));
                            
                        }});
                    System.out.println("Added crawl " + crawlUri + ".");
                    for (final Warc w : c.getWarcs()) {
                        final URI warcUri = createOrLocateTypedResource(crawlUri.toString(), w.getFilename(), new URI(ARCHIVE_IT_WARC_TYPE), false, true, new ResourceInitializer() {

                            @Override
                            public void initializeResource(URI uri) throws UnsupportedEncodingException,
                                    URISyntaxException, FcrepoOperationFailedException, IOException {
                                f4Writer.addLiteralProperty(uri, RDF_TYPE, EXTERNAL_RESOURCE_TYPE);
                                f4Writer.addURIProperty(uri, EXTERNAL_SYSTEM, archiveItSystem.getFedora4Uri());
                                f4Writer.addLiteralProperty(uri, EXTERNAL_ID, w.getURL());
                                f4Writer.addLiteralProperty(uri, MD5_HASH, w.getMD5());
                            }} );
                        System.out.println("Added warc " + warcUri + ".");
                        
                    }
                }
            }
            if (!crawlChecklist.isEmpty()) {
                throw new RuntimeException(crawlChecklist.size() + " crawls exist in fedora for collection " + archiveItCollectionId + " but aren't referenced by ArchiveIt!");
            }
        }
    }
    
    /**
     * Creates an ArchiveIt collection with the given id and title.  The id is expected to
     * be the numeric ID used in the ArchiveIt API.
     */
    public URI findOrCreateArchiveItCollection(final String id, final String title, final File seedList) throws FcrepoOperationFailedException, URISyntaxException, IOException, InterruptedException {
        final URI uri = createOrLocateTypedResource(containerResource(), id, new URI(ARCHIVE_IT_COLLECTION_TYPE), false, true, new ResourceInitializer() {

            @Override
            public void initializeResource(URI uri)
                    throws URISyntaxException, FcrepoOperationFailedException, IOException {
                f4Writer.addLiteralProperty(uri, DC_TITLE, title);
                if (seedList != null) {
                    final URI seedListUri = new URI(uri.toString() + "/collection-seed-list.csv");
                    f4Writer.replaceNonRDFResource(seedListUri, seedList, "application/octet-stream"); // "text/csv" is treated as RDF by fedora
                    f4Writer.addLiteralProperty(new URI(seedListUri.toString() + "/fcr:metadata"), RdfConstants.FILENAME, "collection-seed-list.csv");
                    f4Writer.addURIProperty(uri, RdfConstants.PRES_HAS_SEED_LIST, seedListUri);
                }
                
            } });
        if (seedList != null) {
            final URI seedListUri = new URI(uri.toString() + "/collection-seed-list.csv");
            f4Writer.replaceNonRDFResource(seedListUri, seedList, "application/octet-stream"); // "text/csv" is treated as RDF by fedora
            f4Writer.addLiteralProperty(new URI(seedListUri.toString() + "/fcr:metadata"), RdfConstants.FILENAME, "collection-seed-list.csv");
            f4Writer.addURIProperty(uri, RdfConstants.PRES_HAS_SEED_LIST, seedListUri);
        }
        return uri;
    }
    
    private ExternalSystem findOrCreateArchiveItSystemResource() throws FcrepoOperationFailedException, URISyntaxException, IOException {
        ExternalSystem sys = super.findExternalSystem(archiveItClient.getBaseUrl());
        if (sys == null) {
            sys = createExternalSystem(archiveItClient.getBaseUrl(), "ArchiveIt", true);
            final String addNoteSparql = "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" + 
                    "\n" + 
                    "INSERT DATA { <> skos:note '''ArchiveIt is a service offered by the Internet Archive to archive websites.''' .} ";
            f4Writer.updateWithSparql(sys.getFedora4Uri(), addNoteSparql);
            f4Writer.addURIProperty(sys.getFedora4Uri(), RdfConstants.RDF_TYPE, new URI(RdfConstants.PRESERVATION_PACKAGE_TYPE));
        }
        return sys;
    }
    
    private URI findOrCreateArchiveItContainer() throws URISyntaxException, FcrepoOperationFailedException {
        URI collectionUri = new URI(f4Writer.getBaseUri().toString() + "/" + containerResource()); 
        if (!f4Writer.exists(collectionUri)) {
            // create collection resource
            collectionUri = f4Writer.createNamedResource(containerResource());
            if (!collectionUri.equals( new URI(f4Writer.getBaseUri().toString() + "/" + containerResource()))) {
                throw new RuntimeException("URIs don't match " + collectionUri + "!");
            }
        }
        return collectionUri;

    }
   
    @Override
    protected String containerResource() {
        return "archiveIt";
    }    
    
}
