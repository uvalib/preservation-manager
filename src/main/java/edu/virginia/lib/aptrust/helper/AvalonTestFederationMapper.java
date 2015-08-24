package edu.virginia.lib.aptrust.helper;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

@Deprecated
public class AvalonTestFederationMapper implements FederationMapper {

    private String fcrepoBaseUrl;
    private String avalonRTMPPath;
    private String avalonMasterPath;

    public AvalonTestFederationMapper(final String fcrepoBaseUrl, final String avalonMasterPath, final String avalonRTMPPath) {
        this.fcrepoBaseUrl = fcrepoBaseUrl;
        this.avalonMasterPath = avalonMasterPath;
        this.avalonRTMPPath = avalonRTMPPath;
    }

    public URI mapFileURI(URI fileURI) throws URISyntaxException {
        if (fileURI.getScheme().equals("file")) {
            File file = new File(fileURI);
            if (file.getPath().startsWith(avalonRTMPPath)) {
                return new URI(fcrepoBaseUrl + "/projected-avalontest-rtmp/" + file.getPath().substring(avalonRTMPPath.length()));
            } else if (file.getPath().startsWith(avalonMasterPath)) {
                return new URI(fcrepoBaseUrl + "/projected-avalontest-master/" + file.getName());
            } else {
                throw new RuntimeException("Unable to map file resource to projected URI! (\"" + fileURI + "\")");
            }
        } else {
            throw new IllegalArgumentException(fileURI + " is not a file URI!");
        }
    }

    public String getFederatedURIForFile(File f) {
        try {
            return mapFileURI(f.toURI()).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public File getFileForFederatedURI(String uri) {
        return new File(uri.replace(fcrepoBaseUrl + "/projected-avalontest-rtmp/", avalonRTMPPath).replace(fcrepoBaseUrl + "/projected-avalontest-master/", avalonMasterPath));
    }
}
