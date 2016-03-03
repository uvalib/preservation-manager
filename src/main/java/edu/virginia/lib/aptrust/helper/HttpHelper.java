package edu.virginia.lib.aptrust.helper;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;


public class HttpHelper {

    final private static Logger LOGGER = LoggerFactory.getLogger(HttpHelper.class);

    public static HttpClient createClient() throws IOException {
        File authenticationConfig = new File("auth.properties");
        if (authenticationConfig.exists()) {
            Properties p = new Properties();
            FileInputStream fis = new FileInputStream(authenticationConfig);
            try {
                p.load(fis);
            } finally {
                fis.close();
            }
            CredentialsProvider credsProvider = new BasicCredentialsProvider();
            credsProvider.setCredentials(
                    new AuthScope(p.getProperty("host"), AuthScope.ANY_PORT),
                    new UsernamePasswordCredentials(p.getProperty("username"), p.getProperty("password")));
            CloseableHttpClient httpclient = HttpClients.custom()
                    .setDefaultCredentialsProvider(credsProvider)
                    .build();
            return httpclient;
        } else {
            return HttpClients.createDefault();
        }
    }

    public static void getContentAtURL(final String url, OutputStream os) throws IOException {
        HttpGet get = new HttpGet(url);
        try {
            final HttpResponse response = createClient().execute(get);
            if (response.getStatusLine().getStatusCode() == 401) {
                LOGGER.warn("Received a 403 Forbidden response from " + url + ", you may configure authentication credentials in auth.properties!");
            }
            if (response.getStatusLine().getStatusCode() < 200 || response.getStatusLine().getStatusCode() >= 300) {
                throw new RuntimeException(response.getStatusLine() + " result from request to get " + url);
            }
            IOUtils.copy(response.getEntity().getContent(), os);
        } finally {
            get.releaseConnection();
        }
    }

}
