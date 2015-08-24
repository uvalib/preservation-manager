package edu.virginia.lib.aptrust.ingest;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Date;
import java.util.Properties;

import edu.virginia.lib.aptrust.helper.Fedora4Client;
import edu.virginia.lib.aptrust.helper.FusekiReader;

/**
 * Created by md5wz on 9/16/15.
 */
public class ProductionIngest {

    public static void main(String [] args) throws Exception {
        Properties p = new Properties();
        FileInputStream fis = new FileInputStream("ingest.properties");
        try {
            p.load(fis);
        } finally {
            fis.close();
        }
        
        FusekiReader fuseki = new FusekiReader(getRequiredProperty(p, "triplestore-url"));
        Fedora4Client f4Client = new Fedora4Client(getRequiredProperty(p, "f4-url"));

        PrintWriter report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(getRequiredProperty(p, "ingest-report"), true)));
        report.println("Started " + new Date());
        try {
            
            // TODO: add the ingest operation here

        } finally {
            report.println("Finished " + new Date());
            report.flush();
            report.close();
        }
    }
}
