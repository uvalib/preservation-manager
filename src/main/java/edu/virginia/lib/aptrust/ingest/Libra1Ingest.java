package edu.virginia.lib.aptrust.ingest;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.fcrepo.camel.FcrepoOperationFailedException;

import com.yourmediashelf.fedora.client.FedoraClientException;

import edu.virginia.lib.aptrust.RdfConstants;
import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;
import edu.virginia.lib.aptrust.helper.RightsStatement;

/**
 * The initial modeling of Libra 1 is extremly simplistic.  There is one fedora4
 * object for each object in the Libra fedora 3 instance.
 * 
 * This code doesn't yet extract the rights information from the metadata.
 *   
 * Since we'll likely bag this whole system as a single resource, the resource
 * "libra" which will contain everything will be the only one marked as a 
 * preservation package and for now will have the libra terms of use rights
 * information applied to it.  
 * 
 * There will be two steps to preservation:  
 * 1.  periodic (or event-driven) creation/update of resources in Fedora 4 reflecting
 *     the libra resources
 * 2.  periodic transfers of entire libra collection snapshots to AP Trust
 *
 */
public class Libra1Ingest extends AbstractIngest {

    private File termsOfUse;
    
    private URI collectionUri;
    
    public static void main(String [] args) throws Exception {
        if (args.length != 1) {
            System.err.println("The dump file is a required argument!");
            System.exit(-1);
        }
        File dumpFile = new File(args[0]);
        Properties p = new Properties();
        FileInputStream fis = new FileInputStream("ingest.properties");
        try {
            p.load(fis);
        } finally {
            fis.close();
        }
        
        FusekiReader fuseki = new FusekiReader(getRequiredProperty(p, "triplestore-url"));
        Fedora4Client f4Client = new Fedora4Client(getRequiredProperty(p, "f4-url"));

        new Libra1Ingest(f4Client, fuseki, dumpFile);
    }
    
    public Libra1Ingest(Fedora4Client f4Writer, FusekiReader triplestore, File dumpFile) throws IOException, FcrepoOperationFailedException, URISyntaxException, FedoraClientException {
        super(f4Writer, triplestore);
        termsOfUse = new File("documents/2011-12-Libra-TOU.pdf");
        if (!f4Writer.exists(new URI(f4Writer.getBaseUri().toString() + "/" + containerResource()))) {
            // create collection resource
            collectionUri = f4Writer.createNamedResource(containerResource());
            
            // make it a preservation package
            f4Writer.addURIProperty(collectionUri, RdfConstants.RDF_TYPE, new URI(RdfConstants.PRESERVATION_PACKAGE_TYPE));
            
            // add rights statements
            final URI rights = this.findLibraRightsStatementURI();
            f4Writer.addURIProperty(collectionUri, RdfConstants.RIGHTS, rights);
        }
        addOrReplaceLibraDump(dumpFile);
    }

    public void addOrReplaceLibraDump(File dumpfile) throws FcrepoOperationFailedException, IOException, URISyntaxException {
        if (!dumpfile.getName().endsWith("tar.gz")) {
            throw new RuntimeException("The libra data dump should be a gnu-zipped tar file!");
        }
        final String query = "PREFIX ebucore: <http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#>\n" + 
                "PREFIX fedora: <http://fedora.info/definitions/v4/repository#>\n" + 
                "\n" + 
                "SELECT ?f\n" + 
                "WHERE {\n" + 
                "     ?o fedora:hasParent <http://fedora01.lib.virginia.edu:8080/fcrepo/rest/libra> .\n" + 
                "     ?f <http://www.iana.org/assignments/relation/describedby> ?o .\n" + 
                "     ?f ebucore:filename '''libra-data-directory-dump.tar.gz''' .\n" + 
                "}";
        final String existingUriString = triplestore.getFirstAndOnlyQueryResponse(query).get("f");
        if (existingUriString == null) {
            final URI uri = f4Writer.createNonRDFResource(collectionUri, dumpfile, "application/gzip");
            f4Writer.addLiteralProperty(uri, RdfConstants.FILENAME, "libra-data-directory-dump.tar.gz");
        } else {
            f4Writer.replaceNonRDFResource(new URI(existingUriString), dumpfile, "application/gzip");
        }
        
    }
    
    @Override
    protected String containerResource() {
        return "libra";
    }
    
    /**
     * Creates or locates the Rights statement for Libra content.
     */
    private URI findLibraRightsStatementURI() throws IOException, URISyntaxException, FcrepoOperationFailedException {
        RightsStatement rs = new RightsStatement("http://libra.virginia.edu/terms", "Terms of Use for Libra", "", null, "2011-12");
        URI rsURI = lookupFedora4URI(rs.getIdentifier(), RdfConstants.RIGHTS_STATEMENT);
        if (rsURI == null) {
            
            // create a new one
            rsURI = createResource(rs.getIdentifier(), new URI(RdfConstants.RIGHTS_STATEMENT), false, true);
            f4Writer.addURIProperty(rsURI, RdfConstants.RDF_TYPE, new URI(RdfConstants.CONCEPT));
            rs.writeToFedora(rsURI, f4Writer);

            // add a link to a rendering of the statement
            final URI spreadsheetURI = new URI(f4Writer.createNonRDFResource(rsURI, termsOfUse, "application/pdf").toString() + "/fcr:metadata");
            f4Writer.addLiteralProperty(spreadsheetURI, RdfConstants.FILENAME, termsOfUse.getName());
        }

        return rsURI;
    }

}
