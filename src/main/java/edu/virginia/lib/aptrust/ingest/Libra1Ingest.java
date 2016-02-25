package edu.virginia.lib.aptrust.ingest;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.fcrepo.camel.FcrepoOperationFailedException;

import com.yourmediashelf.fedora.client.FedoraClient;
import com.yourmediashelf.fedora.client.FedoraClientException;
import com.yourmediashelf.fedora.client.FedoraCredentials;

import edu.virginia.lib.aptrust.RdfConstants;
import edu.virginia.lib.aptrust.helper.ExternalSystem;
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
    
    private FedoraClient f3Client;
    
    private URI collectionUri;
    
    public Libra1Ingest(Fedora4Client f4Writer, FusekiReader triplestore) throws IOException, FcrepoOperationFailedException, URISyntaxException {
        super(f4Writer, triplestore);
        
        FileInputStream fis = new FileInputStream("libra-ingest-config.properties");
        try {
            Properties config = new Properties();
            config.load(fis);
            final String fedora3Url = getRequiredProperty(config, "libra-fedora-url");
            f3Client = new FedoraClient(new FedoraCredentials(fedora3Url, getRequiredProperty(config, "wsls-fedora-username"), getRequiredProperty(config, "wsls-fedora-password")));
            termsOfUse = new File(getRequiredProperty(config, "terms-of-use"));
        } finally {
            fis.close();
        }
        if (!f4Writer.exists(new URI(f4Writer.getBaseUri().toString() + "/" + containerResource()))) {
            // create collection resource
            collectionUri = f4Writer.createNamedResource(containerResource());
            
            // make it a preservation package
            f4Writer.addURIProperty(collectionUri, RdfConstants.RDF_TYPE, new URI(RdfConstants.PRESERVATION_PACKAGE_TYPE));
            
            // add rights statements
            final URI rights = this.findLibraRightsStatementURI();
            f4Writer.addURIProperty(collectionUri, RdfConstants.RIGHTS, rights);
        }
    }
    
    /**
     * Creates resource in the Fedora 4 preservation staging repository for
     * each item in the Libra's Fedora instance.
     */
    public void createProxyResources() {
        
        
        // iterate over all objects
        
        // create a fedora 4 resource
        
        
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
    
    /**
     * Creates or locates the fedora external system resource and returns it's URI.
     */
    private ExternalSystem findGenericFedoraExternalSystemURI(final String fedoraBaseUrl) throws FcrepoOperationFailedException, IOException, URISyntaxException, FedoraClientException {
        ExternalSystem sys = super.findExternalSystem(fedoraBaseUrl);
        if (sys == null) {
            final String fedoraVersion = FedoraClient.describeRepository().execute(f3Client).getRepositoryInfo().getRepositoryVersion();
            sys = super.createExternalSystem(fedoraBaseUrl, "Fedora " + fedoraVersion, true);
        }
        return sys;
    }

}
