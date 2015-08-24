package edu.virginia.lib.aptrust.helper;

import java.io.File;

public interface FederationMapper {

    String getFederatedURIForFile(File f);

    File getFileForFederatedURI(String uri);

}
