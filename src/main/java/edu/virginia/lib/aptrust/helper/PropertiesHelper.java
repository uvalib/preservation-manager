package edu.virginia.lib.aptrust.helper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;import java.io.IOException;
import java.util.Properties;

public class PropertiesHelper {
    
    public static String getRequiredProperty(Properties p, String name) {
        if (p.containsKey(name)) {
            return p.getProperty(name);
        } else {
            throw new RuntimeException("Required property \"" + name + "\" not found!");
        }
    }

    public static String getOptionalProperty(Properties p, String name) {
        if (p.containsKey(name)) {
            return p.getProperty(name);
        } else {
            return null;
        }
    }

    
    public static Properties getProperties(String filename) throws IOException {
        Properties p = new Properties();
        FileInputStream fis = new FileInputStream(filename);
        try {
            p.load(fis);
            return p;
        } finally {
            fis.close();
        }
    }
}
