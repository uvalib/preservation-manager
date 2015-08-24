package edu.virginia.lib.aptrust.helper;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import edu.virginia.lib.aptrust.RdfConstants;
import edu.virginia.lib.aptrust.ingest.WSLSIngest;

import org.apache.commons.io.IOUtils;
import org.fcrepo.camel.FcrepoClient;
import org.fcrepo.camel.FcrepoOperationFailedException;
import org.fcrepo.camel.FcrepoResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Fedora4Client {

    final private static Map<String, String> namespaceToPrefixMap = new HashMap<String, String>();
    static {
        namespaceToPrefixMap.put(RdfConstants.SKOS_NAMESPACE, "skos");
        namespaceToPrefixMap.put(RdfConstants.PCDM_NAMESPACE, "pcdm");
        namespaceToPrefixMap.put(RdfConstants.UVA_PRESERVATION_NAMESPACE, "pres");
        namespaceToPrefixMap.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf");
        namespaceToPrefixMap.put("http://purl.org/dc/elements/1.1/", "dc");
        namespaceToPrefixMap.put("http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#", "ebucore");
        namespaceToPrefixMap.put("http://purl.org/dc/terms/", "dcterms");
        namespaceToPrefixMap.put("http://fedora.lib.virginia.edu/wsls/relationships#", "wsls");
        namespaceToPrefixMap.put(RdfConstants.PREMIS_NAMESPACE, "premis");
    }

    final static class Name {

        private String namespace;
        private String localname;
        private String prefix;

        public Name(String uri) {
            for (Map.Entry<String, String> e : namespaceToPrefixMap.entrySet()) {
                if (uri.startsWith(e.getKey())) {
                    namespace = e.getKey();
                    localname = uri.substring(namespace.length());
                    prefix = e.getValue();
                    return;
                }
            }
            throw new RuntimeException(uri + " does not have a registered namespace prefix!");
        }

        public String getNamespace() {
            return namespace;
        }

        public String getName() {
            return localname;
        }

        public String getPrefix() {
            return prefix;
        }

    }

    final private static Logger LOGGER = LoggerFactory.getLogger(Fedora4Client.class);

    private URI baseUri;

    public Fedora4Client(final String baseUrl) throws URISyntaxException {
        this.baseUri = new URI(baseUrl);
    }
    
    public URI getBaseUri() {
    	return this.baseUri;
    }
    
    public boolean exists(final URI uri) throws FcrepoOperationFailedException {
    	FcrepoResponse r = getClient().head(uri);
    	return r.getStatusCode() != 404;
    }

    public URI createResource(final String rootContainer) throws FcrepoOperationFailedException, URISyntaxException {
        FcrepoResponse r = getClient().post(rootContainer != null ? (rootContainer.startsWith(baseUri.toString()) ? new URI(rootContainer) : new URI(baseUri.toString() + "/" + rootContainer)) : baseUri, null, null);
        assertSuccess(r);
        LOGGER.debug("Created new resource " + r.getLocation() + ".");
        return r.getLocation();
    }
    
    public URI createNamedResource(final String path) throws FcrepoOperationFailedException, URISyntaxException {
    	FcrepoResponse r = getClient().put(new URI(baseUri.toString() + "/" + path), null, null);
    	assertSuccess(r);
    	return r.getLocation();
    }

    public void removeProperties(URI subject, String predicate) throws UnsupportedEncodingException, FcrepoOperationFailedException {
        Name n = new Name(predicate);
        final String sparqlUpdate = "PREFIX " + n.getPrefix() + ": <" + n.getNamespace() + ">\n DELETE WHERE { " + n.getPrefix() + ":" + n.getName() + " <" + predicate + "> ?o . }";
        FcrepoResponse r = getClient().patch(subject, new ByteArrayInputStream(sparqlUpdate.getBytes("UTF-8")));
        assertSuccess(r);
    }

    public void addLiteralProperty(URI subject, String predicate, String literal) throws UnsupportedEncodingException, URISyntaxException, FcrepoOperationFailedException {
        Name n = new Name(predicate);
        final String sparqlUpdate = "PREFIX " + n.getPrefix() + ": <" + n.getNamespace() + ">\n INSERT DATA { <> " + n.getPrefix() + ":" + n.getName() + " '''" + literal + "''' . }";
        try {
            FcrepoResponse r = getClient().patch(subject, new ByteArrayInputStream(sparqlUpdate.getBytes("UTF-8")));
            assertSuccess(r);
        } catch (FcrepoOperationFailedException ex) {
            LOGGER.warn("Error for patch of \"" + sparqlUpdate + "\"", ex);
            throw ex;
        }
    }
    
    public void addDateProperty(URI subject, String predicate, Date date) throws UnsupportedEncodingException, URISyntaxException, FcrepoOperationFailedException {
    	SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        Name n = new Name(predicate);
        final String sparqlUpdate = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\nPREFIX " + n.getPrefix() + ": <" + n.getNamespace() + ">\n INSERT DATA { <> " + n.getPrefix() + ":" + n.getName() + " \"" + f.format(date) + "\"^^xsd:dateTime . }";
        try {
            FcrepoResponse r = getClient().patch(subject, new ByteArrayInputStream(sparqlUpdate.getBytes("UTF-8")));
            assertSuccess(r);
        } catch (FcrepoOperationFailedException ex) {
            LOGGER.warn("Error for patch of \"" + sparqlUpdate + "\"", ex);
            throw ex;
        }
    }
    
    public void addIntegerProperty(URI subject, String predicate, long value) throws UnsupportedEncodingException, FcrepoOperationFailedException {
    	Name n = new Name(predicate);
        final String sparqlUpdate = "PREFIX " + n.getPrefix() + ": <" + n.getNamespace() + ">\n INSERT DATA { <> " + n.getPrefix() + ":" + n.getName() + " " + value + " . }";
        try {
            FcrepoResponse r = getClient().patch(subject, new ByteArrayInputStream(sparqlUpdate.getBytes("UTF-8")));
            assertSuccess(r);
        } catch (FcrepoOperationFailedException ex) {
            LOGGER.warn("Error for patch of \"" + sparqlUpdate + "\"", ex);
            throw ex;
        }
    }

    public void updateLiteralProperty(URI subject, String predicate, String literal) throws UnsupportedEncodingException, FcrepoOperationFailedException, URISyntaxException {
        removeProperties(subject, predicate);
        addLiteralProperty(subject, predicate, literal);
    }

    public void addURIProperty(URI subject, String predicate, URI uri) throws UnsupportedEncodingException, URISyntaxException, FcrepoOperationFailedException {
        Name n = new Name(predicate);
        String sparqlUpdate = "PREFIX " + n.getPrefix() + ": <" + n.getNamespace() + ">\n"
                + " INSERT DATA { <> " + n.getPrefix() + ":" + n.getName() + " <" + uri + "> . }";
        try {
	        Name n2 = new Name(uri.toString());
	        sparqlUpdate = "PREFIX " + n.getPrefix() + ": <" + n.getNamespace() + ">\n"
	        		+ "PREFIX " + n2.getPrefix() + ": <" + n2.getNamespace() + ">\n"
	                + " INSERT DATA { <> " + n.getPrefix() + ":" + n.getName() + " " + n2.getPrefix() + ":" + n2.getName() + " . }";
        } catch (RuntimeException ex) {
        	// fine, we won't worry about a prefix for that one...
        }
        try {
            FcrepoResponse r = getClient().patch(subject, new ByteArrayInputStream(sparqlUpdate.getBytes("UTF-8")));
            assertSuccess(r);
        } catch (FcrepoOperationFailedException ex) {
            LOGGER.warn("Error for patch of \"" + sparqlUpdate + "\"", ex);
            throw ex;
        }
    }

    public void updateURIProperty(URI subject, String predicate, URI uri) throws UnsupportedEncodingException, FcrepoOperationFailedException, URISyntaxException {
        removeProperties(subject, predicate);
        addURIProperty(subject, predicate, uri);
    }

    public URI createRedirectNonRDFResource(URI uri, URI parentURI) throws MalformedURLException, FcrepoOperationFailedException, URISyntaxException {
        FcrepoResponse r = getClient().post(parentURI == null ? baseUri : parentURI, null, "message/external-body; access-type=URL; URL=\"" + uri.toURL().toString() + "\"");
        assertSuccess(r);
        return new URI(r.getLocation().toString());
    }

    public URI createNonRDFResource(URI parentURI, File f, String mimeType) throws IOException, FcrepoOperationFailedException, URISyntaxException {
        final FileInputStream fis = new FileInputStream(f);
        try {
        	FcrepoResponse r = getClient().post(parentURI == null ? baseUri : parentURI, fis, mimeType);
        	assertSuccess(r);
            return r.getLocation();
        } finally {
            fis.close();
        }
    }
    
    public URI createNonRDFResource(URI parentURI, String content, String mimeType) throws IOException, FcrepoOperationFailedException, URISyntaxException {
        final ByteArrayInputStream is = new ByteArrayInputStream(content.getBytes());
        try {
        	FcrepoResponse r = getClient().post(parentURI == null ? baseUri : parentURI, is, mimeType);
        	assertSuccess(r);
            return r.getLocation();
        } finally {
            is.close();
        }
    }

    public void export(URI uri, OutputStream os, boolean recurse, boolean skipBinary) throws URISyntaxException, FcrepoOperationFailedException, IOException {
        FcrepoResponse r = getClient().get(new URI(uri.toString() + "/fcr:export?recurse=" + (recurse ? "true" : "false") + "&skipBinary=" + (skipBinary ? "true" : "false")), null, null);
        if (r.getStatusCode() > 299 || r.getStatusCode() < 200) {
            throw new RuntimeException("Status code " + r.getStatusCode() + " from export request!");
        }
        IOUtils.copy(r.getBody(), os);
    }

    private FcrepoClient getClient() {
        return new FcrepoClient(null, null, baseUri.toString(), true);
    }

    public boolean isRdfResource(URI uri) throws FcrepoOperationFailedException, URISyntaxException {
        FcrepoResponse r = getClient().get(uri, "application/n-triples", null);
        return success(r);
    }

    public Model getAllProperties(URI requestUri) throws FcrepoOperationFailedException, IOException {
        Model model = ModelFactory.createDefaultModel();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(getClient().get(requestUri, "application/n-triples", null).getBody(), baos);
        model.read(new ByteArrayInputStream(baos.toByteArray()), null, "N-TRIPLE");
        return model;
    }

    public static Set<RDFNode> getPropertyValues(Model m, URI subjectURI, String propertyUri) {
        StmtIterator it = m.getResource(String.valueOf(subjectURI)).listProperties(m.createProperty(propertyUri));
        Set<RDFNode> results = new HashSet<RDFNode>();
        while (it.hasNext()) {
            results.add(it.next().getObject());
        }
        return results;
    }

    public static String getFirstPropertyValue(Model m, URI subjectURI, String propertyUri) {
        Set<RDFNode> set = getPropertyValues(m, subjectURI, propertyUri);
        if (set.isEmpty()) {
            return null;
        } else {
            final RDFNode first = set.iterator().next();
            if (first.isResource()) {
                return first.asResource().getURI();
            } else if (first.isLiteral()) {
                return first.asLiteral().getString();
            } else {
                return first.toString();
            }
        }
    }

    public static boolean hasType(Model m, String uriStr, String rdfType) throws URISyntaxException {
        for (RDFNode n : getPropertyValues(m, new URI(uriStr), RdfConstants.RDF_TYPE)) {
            if (n.isResource() && n.asResource().getURI().toString().equals(rdfType)) {
                return true;
            }
        }
        return false;
    }

    public Set<RDFNode> getPropertyValues(URI requestUri, URI subjectURI, String propertyUri) throws FcrepoOperationFailedException, IOException {
        Model model = ModelFactory.createDefaultModel();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(getClient().get(requestUri, "application/n-triples", null).getBody(), baos);
        //System.out.println(baos.toString("UTF-8"));
        model.read(new ByteArrayInputStream(baos.toByteArray()), null, "N-TRIPLE");
        StmtIterator it = model.getResource(String.valueOf(subjectURI)).listProperties(model.createProperty(propertyUri));
        Set<RDFNode> results = new HashSet<RDFNode>();
        while (it.hasNext()) {
            results.add(it.next().getObject());
        }
        return results;
    }

    /**
     * Gets the single property value, throws exception if there isn't exactly 1 value.
     * present.
     */
    public String getSingleRequiredPropertyValue(URI requestUri, URI subjectUri, String propertyUri) throws FcrepoOperationFailedException, IOException {
        Set<RDFNode> results = getPropertyValues(requestUri, subjectUri, propertyUri);
        if (results.size() == 1) {
            final RDFNode node = results.iterator().next();
            if (node.isURIResource()) {
                return node.asResource().getURI();
            } else if (node.isLiteral()) {
                return node.asLiteral().getString();
            } else {
                throw new IllegalStateException();
            }
        } else {
            throw new RuntimeException(results.size() + " values exist for the " + propertyUri + " property of "  + subjectUri + " at " + requestUri + "!");
        }
    }

    private boolean success(FcrepoResponse r) {
        return r.getStatusCode() >= 200 && r.getStatusCode() < 300;
    }

    private void assertSuccess(FcrepoResponse r) {
        if (!success(r)) {
            throw new RuntimeException("error code " + r.getStatusCode() + " from request " + r.getUrl());

        }
    }
}
