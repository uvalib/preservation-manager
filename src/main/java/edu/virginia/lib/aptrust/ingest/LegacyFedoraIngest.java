package edu.virginia.lib.aptrust.ingest;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getOptionalProperty;
import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.fcrepo.client.FcrepoOperationFailedException;

import edu.virginia.lib.aptrust.RdfConstants;
import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;
import edu.virginia.lib.aptrust.helper.SolrReader;

/**
 * This class allows resources in legacy Fedora repositories to be 
 * ingested.  Since none of them are to be "preserved" by default,
 * this only serves the need to register the existance and disposition
 * of such content. 
 */
public class LegacyFedoraIngest extends AbstractIngest {

    public static void main(String [] args) throws Exception {
        Properties p = new Properties();
        FileInputStream fis = new FileInputStream("fedora-prod01-ingest.properties");
        try {
            p.load(fis);
        } finally {
            fis.close();
        }
        
        FusekiReader fuseki = new FusekiReader(getRequiredProperty(p, "triplestore-url"));
        Fedora4Client f4Client = new Fedora4Client(getOptionalProperty(p, "f4-username"), getOptionalProperty(p, "f4-password"), getRequiredProperty(p, "f4-url"));

        LegacyFedoraIngest i = new LegacyFedoraIngest(f4Client, fuseki, new SolrReader(getRequiredProperty(p, "solr-url"), true));
        i.ingestAllPids();
    }
    
    private SolrReader solr;
    
    public LegacyFedoraIngest(Fedora4Client f4Writer, FusekiReader triplestore, SolrReader s) {
        super(f4Writer, triplestore);
        solr = s;
        
    }


    public void ingestAllPids() throws FcrepoOperationFailedException, URISyntaxException, IOException, SolrServerException, InterruptedException {
        URI collectionUri = new URI(f4Writer.getBaseUri().toString() + "/" + containerResource());
        if (!f4Writer.exists(collectionUri)) {
            // create collection resource
            collectionUri = f4Writer.createNamedResource(containerResource());
            
            // make it an external system
            f4Writer.addLiteralProperty(collectionUri, RdfConstants.DC_IDENTIFIER, "http://fedora-prod01.lib.virginia.edu/");
            f4Writer.addURIProperty(collectionUri, RdfConstants.RDF_TYPE, new URI(RdfConstants.EXTERNAL_SYSTEM_TYPE));
            f4Writer.addLiteralProperty(collectionUri, RdfConstants.DCTERMS_DESCRIPTION, "Fedora 3.2.1 - legacy content production repository");
        }
        
        BufferedReader r = new BufferedReader(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("fedora-prod01.csv")));
        try {
            String line = null;
            while ((line = r.readLine()) != null) {
                final int firstComma = line.indexOf(',');
                if (firstComma < 0) {
                    System.err.println("Unparsible line: " + line);
                    System.exit(-1);
                }
                if (line.startsWith("info:fedora/uva-lib:")) {
                    final String pid = line.substring(12,firstComma);
                    final String title = line.substring(firstComma + 1);
                    final String virgoUrl = getVirgoUrl(pid);
                    final URI uri = createOrLocateTypedResource(collectionUri.toString(), pid, new URI(RdfConstants.EXTERNAL_RESOURCE_TYPE), false, true);
                    f4Writer.addURIProperty(uri, RdfConstants.EXTERNAL_SYSTEM, collectionUri);
                    if (title != null) {
                        f4Writer.addLiteralProperty(uri,  RdfConstants.DC_TITLE, title);
                    }
                    if (virgoUrl != null) {
                        f4Writer.addLiteralProperty(uri, RdfConstants.PRES_HAS_VIRGO_VIEW, virgoUrl);
                    }
                    
                    if (title.equals("null")) {
                        System.out.println(pid + "  \"" + (virgoUrl != null ? virgoUrl : ""));
                    } else {
                        System.out.println(pid + " - \"" + title + "\" " + (virgoUrl != null ? virgoUrl : ""));
                    }
                } else {
                    System.err.println("Skipping " + line);
                }
            }
        } finally {
            r.close();
        }
    }
    
    @Override
    protected String containerResource() {
        return "fedora-prod01";
    }
    
    private String getVirgoUrl(final String pid) throws SolrServerException {
        Iterator<SolrDocument> solrIt = solr.getRecordsForQuery("id:\"" + pid + "\"");
        if (solrIt.hasNext()) {
            return "http://search.lib.virginia.edu/catalog/" + pid;
        } 
        return null;
    }
    
}
