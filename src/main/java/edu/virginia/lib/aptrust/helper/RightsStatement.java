package edu.virginia.lib.aptrust.helper;

import org.fcrepo.camel.FcrepoOperationFailedException;

import edu.virginia.lib.aptrust.RdfConstants;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by md5wz on 8/31/15.
 */
public class RightsStatement {

    String preferredLabel;
    String definition;
    String note;
    String version;
    String identifier;

    public RightsStatement(String id, String label, String definition, String note, String version) {
    this.identifier = id;
        this.preferredLabel = label;
        this.definition = definition;
        this.note = note;
        this.version = version;
    }

    public void writeToFedora(final URI rsURI, final Fedora4Client f4Writer) throws UnsupportedEncodingException, FcrepoOperationFailedException, URISyntaxException {
        if (getPreferredLabel() != null) {
            f4Writer.addLiteralProperty(rsURI, RdfConstants.SKOS_NAMESPACE + "prefLabel", getPreferredLabel());
            f4Writer.addLiteralProperty(rsURI, RdfConstants.DC_TITLE, getPreferredLabel());
        }
        if (getDefinition() != null) {
            f4Writer.addLiteralProperty(rsURI, RdfConstants.DEFINITION, getDefinition());
        }
        if (getNote() != null) {
            f4Writer.addLiteralProperty(rsURI, RdfConstants.SKOS_NAMESPACE + "note", getNote());
        }
        if (getVersion() != null) {
            f4Writer.addLiteralProperty(rsURI, RdfConstants.HAS_VERSION, getVersion());
        }
    }

    public String getPreferredLabel() {
        return preferredLabel;
    }

    public String getDefinition() {
        return definition;
    }

    public String getNote() {
        return note;
    }

    public String getVersion() {
        return version;
    }

    public String getIdentifier() {
        return identifier;
    }

    public boolean equals(RightsStatement other) {
        return this.identifier.equals(other);
    }

    public int hashCode() {
        return identifier.hashCode();
    }

    public static RightsStatement CC_ATTRIBUTION = new RightsStatement("CC BY", "Creative Commons Attribution", "CC BY license :  This license lets others distribute, remix, tweak, and build upon your work, even commercially, as long as they credit you for the original creation. This is the most accommodating of licenses offered. Recommended for maximum dissemination and use of licensed materials.\" For more on CC licenses see http://creativecommons.org/licenses/", null, "3.0" );

}
