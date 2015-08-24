package edu.virginia.lib.aptrust.helper;

import java.io.File;

/**
 * Represents a file that is Federated in fedora.
 */
public class FederatedFile {

    private String uri;

    private File file;

    public FederatedFile(File f, FederationMapper fm) {
        this.file = f;
        this.uri = fm.getFederatedURIForFile(f);
    }

    public FederatedFile(File f, String uri) {
        file = f;
        this.uri = uri;
    }

    /**
     * Gets the file for the federated file.  This will only be an accessible path from the system
     * running fedora 4.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns the fedora 4 uri for the federated file.  This should be a valid (as much as a federated resource
     * can be) fedora 4 resource URI.
     */
    public String getURI() {
        return uri;
    }

}
