package edu.virginia.lib.aptrust.ingest;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.yourmediashelf.fedora.client.FedoraClient;
import com.yourmediashelf.fedora.client.FedoraClientException;
import com.yourmediashelf.fedora.client.FedoraCredentials;

import edu.virginia.lib.aptrust.RdfConstants;
import edu.virginia.lib.aptrust.helper.ExternalSystem;
import edu.virginia.lib.aptrust.helper.FederatedFile;
import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;
import edu.virginia.lib.aptrust.helper.ResourceIndexHelper;
import edu.virginia.lib.aptrust.helper.RightsStatement;

/**
 * Created by md5wz on 9/14/15.
 */
public class WSLSIngest extends AbstractIngest {
    
    final private static Logger LOGGER = LoggerFactory.getLogger(WSLSIngest.class);

    public static final String IS_ANCHOR_SCRIPT_FOR = "http://fedora.lib.virginia.edu/wsls/relationships#isAnchorScriptFor";
    public static final String HAS_ANCHOR_SCRIPT = "http://fedora.lib.virginia.edu/wsls/relationships#hasAnchorScript";

    private String fedora3Url;
    
    private FedoraClient f3Client;

    private String spreadsheetFilename;

    private String preservationMountPath;
    
    private File termsOfUsePDF;
    
    private File deedOfGift;
    
    private PrintWriter p;

    public WSLSIngest(Fedora4Client f4Writer, FusekiReader triplestore, PrintWriter report) throws FcrepoOperationFailedException, URISyntaxException, IOException {
        super(f4Writer, triplestore);
        
        FileInputStream fis = new FileInputStream("wsls-ingest-config.properties");
        try {
            Properties config = new Properties();
            config.load(fis);
            preservationMountPath = getRequiredProperty(config, "wsls-preservation-mount-path");
            fedora3Url = getRequiredProperty(config, "wsls-fedora-url");
            spreadsheetFilename = getRequiredProperty(config, "wsls-spreadsheet-filename");
            f3Client = new FedoraClient(new FedoraCredentials(fedora3Url, getRequiredProperty(config, "wsls-fedora-username"), getRequiredProperty(config, "wsls-fedora-password")));
            termsOfUsePDF = new File(getRequiredProperty(config, "wsls-terms-of-use-path"));
            if (!termsOfUsePDF.exists()) {
                throw new RuntimeException("Terms of use PDF, " + termsOfUsePDF.getAbsolutePath() + ", doesn't exist!");
            }
            deedOfGift = new File(getRequiredProperty(config, "wsls-deed-of-gift-path"));
            if (!deedOfGift.exists()) {
                throw new RuntimeException("Deed of gift PDF for WSLS, " + deedOfGift.getAbsolutePath() + ", doesn't exist!");
            }
        } finally {
            fis.close();
        }
        p = report;
        if (!f4Writer.exists(new URI(f4Writer.getBaseUri().toString() + "/" + containerResource()))) {
        	f4Writer.createNamedResource(containerResource());
        }

    }
    
	@Override
	protected String containerResource() {
		return "wsls";
	}

    /**
     * This walks through all the files in the WSLS federation space and creates pres:File resources for them.
     * It also identifies those that are preservation master files and marks them as "pres:PreservationPackage"
     * resources.
     */
    public void createPreservationPackagesForFiles(String uri) throws FcrepoOperationFailedException, IOException, URISyntaxException, InterruptedException {
        Model m = f4Writer.getAllProperties(new URI(uri + "/fcr:metadata"));
        if (Fedora4Client.hasType(m, uri.toString(), "http://fedora.info/definitions/v4/repository#Container")) {
            for (RDFNode n : Fedora4Client.getPropertyValues(m, new URI(uri), RdfConstants.LDP_CONTAINS)) {
                if (n.isResource()) {
                    createPreservationPackagesForFiles(n.asResource().getURI());
                } else {
                    throw new RuntimeException("ldp:contained resources must be resources!");
                }
            }
        } else {
            FederatedFile ff = new FederatedFile(new File(preservationMountPath + "/" + uri.substring(uri.indexOf("/av-masters/") + 12)), uri);
            p.println(ff.getFile().getPath() + "," + ff.getURI() + "," +  isPreservationMasterFile(ff));
            createOrLocateFileResource(ff, isPreservationMasterFile(ff), true);
        }
    }

    /**
     * Determine whether a FederatedFile is a preservation master file.  The current implementation
     * bases this determination on file naming conventions.
     */
    private boolean isPreservationMasterFile(FederatedFile ff) {
        Pattern p = Pattern.compile("^.*\\.[pP][dD][fF]$|^[^LH]*\\.[mM][oO][vV]$");
        Matcher m = p.matcher(ff.getFile().getName());
        return m.matches();
    }

    public void ingestSpreadsheet() throws Exception {
        URI wslsRightsStatementURI = new URI(findWSLSRightsStatementURI());
        URI thirdPartyCopyrightStatementURI = new URI(findThirdPartyRightsStatementURI());

        ExternalSystem fedoraProd02SystemResource = findGenericFedoraExternalSystemURI(fedora3Url);

        findCollectionResource(spreadsheetFilename);

        FileInputStream fis = new FileInputStream(spreadsheetFilename);
        try {
            HSSFWorkbook workbook = new HSSFWorkbook(fis);
            HSSFSheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rowIt = sheet.iterator();
            if (rowIt.hasNext()) {
                rowIt.next();
            } else {
                LOGGER.warn("No rows in spreadsheet!");
            }
            while (rowIt.hasNext()) {
                Row row = rowIt.next();
                if (row.getRowNum() != 0) {
                    final String virgoUrl = getText(row.getCell(40));
                    final String pid = virgoUrl != null ? virgoUrl.substring("http://search.lib.virginia.edu/catalog/".length()) : null;
                    final String wslsID = getText(row.getCell(6));
                    final boolean copyrighted = !"L".equals(getText(row.getCell(3)));
                    final String title = getText(row.getCell(12));
                    final List<String> pdfURIs = getPDFURIs(wslsID);
                    final List<String> movieURIs = getMovieURIs(wslsID);

                    if (pid == null) {
                    	// don't do anything... this is an unprocessed WSLS item.
                    } else {
                        URI id = findOrCreateFedoraExternalResource(pid, fedoraProd02SystemResource, true, true);
                        LOGGER.info("Spreadsheet row " + row.getRowNum() + ", " + wslsID + " --> " + id.toString());
                        f4Writer.addURIProperty(id, RdfConstants.RIGHTS, wslsRightsStatementURI);
                        if (copyrighted) {
                            f4Writer.addURIProperty(id, RdfConstants.RIGHTS, thirdPartyCopyrightStatementURI);
                        }

                        for (String movieURI : movieURIs) {
                        	f4Writer.updateURIProperty(id, RdfConstants.HAS_FILE, new URI(movieURI));
                        }
                        f4Writer.updateLiteralProperty(id, RdfConstants.DC_TITLE, title);
                        f4Writer.addLiteralProperty(id, RdfConstants.DC_IDENTIFIER, wslsID);

                        for (String anchorScriptPid : ResourceIndexHelper.getSubjects(f3Client, IS_ANCHOR_SCRIPT_FOR, pid)) {
                            final URI scriptId = findOrCreateFedoraExternalResource(anchorScriptPid, fedoraProd02SystemResource, true, true);
                            f4Writer.addLiteralProperty(scriptId, RdfConstants.DC_IDENTIFIER, wslsID);
                            for (String pdfURI : pdfURIs) {
                            	f4Writer.updateURIProperty(scriptId, RdfConstants.HAS_FILE, new URI(pdfURI));
                            }
                            f4Writer.addURIProperty(scriptId, RdfConstants.RIGHTS, wslsRightsStatementURI);
                            if (copyrighted) {
                                f4Writer.addURIProperty(scriptId, RdfConstants.RIGHTS, thirdPartyCopyrightStatementURI);
                            }
                            f4Writer.addURIProperty(scriptId, IS_ANCHOR_SCRIPT_FOR, id);
                            f4Writer.addLiteralProperty(scriptId, RdfConstants.DC_TITLE, "Anchor script for clip titled \"" + title + "\"");
                            f4Writer.addURIProperty(id, HAS_ANCHOR_SCRIPT, scriptId);
                        }
                    }
                }

            }
        } finally {
            fis.close();
        }
    }
    
    private List<String> getMovieURIs(final String wslsID) throws IOException {
    	final List<String> results = new ArrayList<String>();
    	for (Map<String, String> movies : triplestore.getQueryResponse("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "SELECT ?f\n" +
                "WHERE {\n" +
                "  ?f rdf:type <http://fedora.lib.virginia.edu/preservation#File> .\n" +
                "  ?f <http://fedora.lib.virginia.edu/preservation#hasBinary> ?s .\n" +
                "  ?s <http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#filename> '" + wslsID + ".mov'\n" +
                "}")) {
    		results.add(movies.get("f"));
    	}
    	return results;
    }
    
    private List<String> getPDFURIs(final String wslsID) throws IOException, FcrepoOperationFailedException, URISyntaxException {
    	List<String> pdfs = new ArrayList<String>();
    	List<Map<String, String>> result = triplestore.getQueryResponse("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "SELECT ?f\n" +
                "WHERE {\n" +
                "  ?f rdf:type <http://fedora.lib.virginia.edu/preservation#File> .\n" +
                "  ?f <http://fedora.lib.virginia.edu/preservation#hasBinary> ?s .\n" +
                "  ?s <http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#filename> '" + wslsID + ".pdf'\n" +
                "}");
    	if (result.size() == 1) {
    		pdfs.add(result.get(0).get("f"));
    		return pdfs;
    	} else if (result.size() == 0) {
    		return pdfs;
    	} else {
    		for (Map<String, String> r : result) {
    			if (f4Writer.getSingleRequiredPropertyValue(new URI(r.get("f")), new URI(r.get("f")), RdfConstants.DC_IDENTIFIER).contains("corrected")) {
    				LOGGER.info("Selected " + r.get("f") + " as the correct PDF to link.");
    				return Collections.singletonList(r.get("f"));
    			}
    			pdfs.add(r.get("f"));
    		}
    		return pdfs;
    	}

    }

    public String getText(Cell c) {
        if (c == null) {
            return null;
        }
        switch (c.getCellType()) {
            case Cell.CELL_TYPE_STRING :
                return c.getStringCellValue();
            case Cell.CELL_TYPE_NUMERIC :
                return new DataFormatter().formatCellValue(c);
            case Cell.CELL_TYPE_BLANK :
                return "";
            case Cell.CELL_TYPE_BOOLEAN :
                return String.valueOf(c.getBooleanCellValue());
            case Cell.CELL_TYPE_ERROR :
                return String.valueOf("ERROR: " + c.getErrorCellValue());
            case Cell.CELL_TYPE_FORMULA :
                return c.getCellFormula();
            default:
                throw new RuntimeException("Unknown cell type " + c.getCellType() + " (" + c.getRowIndex() + ", " + c.getColumnIndex() + ")");
        }
    }

    /**
     * Locates or creates a resource representing the WSLS collection master spreadsheet.
     */
    private String findCollectionResource(String spreadsheetFilename) throws InterruptedException, IOException, FcrepoOperationFailedException, URISyntaxException {
        File f = new File(spreadsheetFilename);
        final String id = f.getName();
        return createOrLocateFileResource(f, "application/vnd.ms-excel", true, true).toString();
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

    /**
     * Creates or locates the Rights statement for materials copyrighted by a third party.
     */
    private String findThirdPartyRightsStatementURI() throws IOException, URISyntaxException, FcrepoOperationFailedException {
        RightsStatement rs = new RightsStatement("Third-Party Copyright", "Third-Party Copyright", "The work is know or believed to be under copyright by a third party for whom all but fair-use rights are reserved.", null, null);
        URI rsURI = lookupFedora4URI(rs.getIdentifier(), RdfConstants.RIGHTS_STATEMENT);
        if (rsURI == null) {
            // create a new one
            rsURI = createResource(containerResource(), rs.getIdentifier(), new URI(RdfConstants.RIGHTS_STATEMENT), false, true);
            f4Writer.addURIProperty(rsURI, RdfConstants.RDF_TYPE, new URI(RdfConstants.CONCEPT));
            rs.writeToFedora(rsURI, f4Writer);
        }

        return rsURI.toString();
    }

    /**
     * Creates or locates the Rights statement for materials in the WSLS collection.
     */
    private String findWSLSRightsStatementURI() throws IOException, URISyntaxException, FcrepoOperationFailedException {
        RightsStatement rs = new RightsStatement("WSLS Terms of Use", "WSLS Terms of Use", "Each user of the WSLS materials must individually evaluate any copyright or privacy issues that might pertain to the intended uses of these materials, including fair use.", null, null);
        URI rsURI = lookupFedora4URI(rs.getIdentifier(), RdfConstants.RIGHTS_STATEMENT);
        if (rsURI == null) {
            // create a new one
            rsURI = createResource(containerResource(), rs.getIdentifier(), new URI(RdfConstants.RIGHTS_STATEMENT), false, true);
            f4Writer.addURIProperty(rsURI, RdfConstants.RDF_TYPE, new URI(RdfConstants.CONCEPT));
            rs.writeToFedora(rsURI, f4Writer);

            // add the copyright transfer
            final URI spreadsheetURI = new URI(f4Writer.createNonRDFResource(rsURI, deedOfGift, "application/pdf").toString() + "/fcr:metadata");
            f4Writer.addLiteralProperty(spreadsheetURI, RdfConstants.FILENAME, deedOfGift.getName());


            // add the PDF version
            final URI termsOfUseURI = new URI(f4Writer.createNonRDFResource(rsURI, termsOfUsePDF, "application/pdf").toString() + "/fcr:metadata");
            f4Writer.addLiteralProperty(termsOfUseURI, RdfConstants.FILENAME, termsOfUsePDF.getName());
        }

        return rsURI.toString();
    }

}
