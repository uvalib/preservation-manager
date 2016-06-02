package edu.virginia.lib.aptrust.migrate;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getOptionalProperty;
import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

import java.io.FileInputStream;
import java.util.Properties;

import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;
import edu.virginia.lib.aptrust.ingest.WSLSIngest;

public class MigrateFromFederation {
    
    public static void main(String [] args) throws Exception {
        Properties p = new Properties();
        FileInputStream fis = new FileInputStream("production-ingest.properties");
        try {
            p.load(fis);
        } finally {
            fis.close();
        }
        
        FusekiReader fuseki = new FusekiReader(getRequiredProperty(p, "triplestore-url"));
        Fedora4Client f4Client = new Fedora4Client(getOptionalProperty(p, "f4-username"), getOptionalProperty(p, "f4-password"), getRequiredProperty(p, "f4-url"));
        
        new WSLSIngest(f4Client, fuseki, null).migratePreservationPackagesForFiles("http://fedora01.lib.virginia.edu:8080/fcrepo/rest/av-masters/WSLS_archive");
        
    }
    
}
