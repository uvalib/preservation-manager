package edu.virginia.lib.aptrust.ingest;

import com.yourmediashelf.fedora.client.FedoraClient;
import com.yourmediashelf.fedora.client.FedoraClientException;

import edu.virginia.lib.aptrust.RdfConstants;
import edu.virginia.lib.aptrust.helper.ExternalSystem;
import edu.virginia.lib.aptrust.helper.FederatedFile;
import edu.virginia.lib.aptrust.helper.FederationMapper;
import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;
import edu.virginia.lib.aptrust.helper.RightsAssessor;
import edu.virginia.lib.aptrust.helper.RightsStatement;
import edu.virginia.lib.aptrust.helper.SolrReader;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

/**
 * A class that allows content for which Avalon is the system of record to
 * be included in a Fedora 4 repository for the purpose of preservation
 * management.
 */
public class AvalonIngest extends AbstractIngest {

    final private static Logger LOGGER = LoggerFactory.getLogger(AvalonIngest.class);

    private SolrReader solr;

    private FedoraClient f3Client;

    private RightsAssessor rights;

    private ExternalSystem avalon;

    private FederationMapper fm;

    public AvalonIngest(final SolrReader s, FusekiReader fuseki, FederationMapper fm, Fedora4Client f4Writer, FedoraClient f3Client, final String avalonBaseUrl) throws FcrepoOperationFailedException, IOException, URISyntaxException, FedoraClientException {
        super(f4Writer, fuseki);
        this.fm = fm;
        this.solr = s;
        this.f3Client = f3Client;
        rights = new RightsAssessor();
        avalon = super.findExternalSystem(avalonBaseUrl);
        if (avalon == null) {
            final String fedoraVersion = FedoraClient.describeRepository().execute(f3Client).getRepositoryInfo().getRepositoryVersion();
            avalon = super.createExternalSystem(avalonBaseUrl, "Fedora " + fedoraVersion + ", under the Avalon Media System.  http://www.avalonmediasystem.org/", true);
        }
        if (!f4Writer.exists(new URI(f4Writer.getBaseUri().toString() + "/" + containerResource()))) {
        	f4Writer.createNamedResource(containerResource());
        }
    }
    
    protected String containerResource() {
    	return "avalon";
    }

    public void syncToFedora4(String pid) throws SolrServerException, IOException, FcrepoOperationFailedException, URISyntaxException, FedoraClientException, InterruptedException {
        final SolrDocument rootDoc = solr.getRecordsForQuery("id:\"" + pid + "\"").next();

        // Create the object
        URI id = findOrCreateFedoraExternalResource(pid, avalon, true, true);

        // Update the title
        final String title = (String) rootDoc.getFirstValue("title_tesi");
        f4Writer.updateLiteralProperty(id, RdfConstants.DC_TITLE, title);
        applyRightsStatement(id, rights.getRightsStatementForAvalonResource(rootDoc));

        LOGGER.info(pid);

        Iterator<SolrDocument> partsIt = solr.getRecordsForQuery("is_part_of_ssim:\"info:fedora/" + pid + "\"");
        while (partsIt.hasNext()) {
            final SolrDocument part = partsIt.next();
            final String partPid = (String) part.getFirstValue("id");
            LOGGER.debug("PART: " + partPid);

            final URI partId = findOrCreateFedoraExternalResource(partPid, avalon, true, true);
            final URI masterFileId = createOrLocateFileResource(new FederatedFile(new File(getDatastreamContent(partPid, "masterFile")), fm), true, true);
            f4Writer.updateURIProperty(partId, RdfConstants.HAS_FILE, masterFileId);
            f4Writer.updateLiteralProperty(partId, RdfConstants.DC_TITLE, title + " - Master File " + partPid);
            f4Writer.updateURIProperty(id, RdfConstants.PCDM_HAS_MEMBER, partId);
            applyRightsStatement(partId, rights.getRightsStatementForAvalonResource(part));
            applyRightsStatement(masterFileId, rights.getRightsStatementForAvalonResource(part));

            Iterator<SolrDocument> derivativesIt = solr.getRecordsForQuery("is_derivation_of_ssim:\"info:fedora/" +  partPid + "\"");
            while (derivativesIt.hasNext()) {
                final SolrDocument derivative = derivativesIt.next();
                final String derivativePid = (String) derivative.getFirstValue("id");
                LOGGER.debug("DERIVATIVE: " + derivativePid);

                final URI derivativeId = findOrCreateFedoraExternalResource(derivativePid, avalon, true, true);
                final URI derivativeFileId = createOrLocateFileResource(new FederatedFile(new File(getDatastreamContent(derivativePid, "derivativeFile")), fm), false, true);
                f4Writer.updateURIProperty(derivativeId, RdfConstants.HAS_FILE, derivativeFileId);
                f4Writer.updateLiteralProperty(derivativeId, RdfConstants.DC_TITLE, title + " - Derivative File " + derivativePid);
                f4Writer.updateURIProperty(derivativeFileId, RdfConstants.IS_DERIVED_FROM, masterFileId);
                f4Writer.updateURIProperty(partId, RdfConstants.PCDM_HAS_MEMBER, derivativeId);
                applyRightsStatement(derivativeId, rights.getRightsStatementForAvalonResource(derivative));
                applyRightsStatement(derivativeFileId, rights.getRightsStatementForAvalonResource(part));
             }
        }
    }

    /**
     * TODO: this tramples any other manually applied rights statements
     * @param resourceId
     * @param rs
     * @throws IOException
     * @throws URISyntaxException
     * @throws FcrepoOperationFailedException
     */
    private void applyRightsStatement(final URI resourceId, RightsStatement rs) throws IOException, URISyntaxException, FcrepoOperationFailedException {
        URI rsURI = lookupFedora4URI(rs.getIdentifier(), RdfConstants.RIGHTS_STATEMENT);
        if (rsURI == null) {
            // create a new one
            rsURI = createResource(containerResource(), rs.getIdentifier(), new URI(RdfConstants.RIGHTS_STATEMENT), false, true);
            f4Writer.addURIProperty(rsURI, RdfConstants.RDF_TYPE, new URI(RdfConstants.CONCEPT));
            if (rs.getPreferredLabel() != null) {
                f4Writer.addLiteralProperty(rsURI, RdfConstants.SKOS_NAMESPACE + "prefLabel", rs.getPreferredLabel());
                f4Writer.addLiteralProperty(rsURI, RdfConstants.DC_TITLE, rs.getPreferredLabel());
            }
            if (rs.getDefinition() != null) {
                f4Writer.addLiteralProperty(rsURI, RdfConstants.DEFINITION, rs.getDefinition());
            }
            if (rs.getNote() != null) {
                f4Writer.addLiteralProperty(rsURI, RdfConstants.SKOS_NAMESPACE + "note", rs.getNote());
            }
            if (rs.getVersion() != null) {
                f4Writer.addLiteralProperty(rsURI, RdfConstants.HAS_VERSION, rs.getVersion());
            }
        }
        f4Writer.removeProperties(resourceId, RdfConstants.RIGHTS);
        f4Writer.addURIProperty(resourceId, RdfConstants.RIGHTS, rsURI);
    }

    private String getDatastreamContent(final String fedora3Pid, final String dsId) throws FedoraClientException, IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(FedoraClient.getDatastreamDissemination(fedora3Pid, dsId).execute(f3Client).getEntityInputStream(), baos);
        return new String(baos.toByteArray(), "UTF-8");
    }

}
