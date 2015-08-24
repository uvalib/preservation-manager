package edu.virginia.lib.aptrust.helper;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FusekiReader {

	final private static Logger LOGGER = LoggerFactory.getLogger(FusekiReader.class);
	
    private String fusekiBaseUrl;

    private HttpClient client = createClient();

    private static HttpClient createClient() {
        return HttpClients.createDefault();
    }

    public FusekiReader(final String baseUrl) {
        this.fusekiBaseUrl = baseUrl;
    }

    public Map<String, String> getFirstAndOnlyQueryResponse(final String query) throws IOException {
        final String queryUrl = getFusekiBaseUrl() + "/query?query=" + URLEncoder.encode(query, "UTF-8") +
                "&default-graph-uri=&output=csv&stylesheet=";
        HttpGet get = new HttpGet(queryUrl);
        try {
            HttpResponse r = client.execute(get);
            List<String> lines = IOUtils.readLines(r.getEntity().getContent());
            if (lines.size() > 2) {
                throw new RuntimeException("More than one record mached query! " + query);
            } else if (lines.size() < 2) {
                return Collections.emptyMap();
            }
            String[] keys = splitCSV(lines.get(0));
            String[] values = splitCSV(lines.get(1));
            Map<String, String> result = new HashMap<String, String>();
            for (int i = 0; i < keys.length; i++) {
                result.put(keys[i], values[i]);
            }
            return result;
        } finally {
            get.releaseConnection();
        }

    }

    public List<Map<String, String>> getQueryResponse(final String query) throws IOException {
        final String queryUrl = getFusekiBaseUrl() + "/query?query=" + URLEncoder.encode(query, "UTF-8") +
                "&default-graph-uri=&output=csv&stylesheet=";
        HttpGet get = new HttpGet(queryUrl);
        try {
            HttpResponse r = client.execute(get);
            List<String> lines = IOUtils.readLines(r.getEntity().getContent());
            if (lines.size() < 2) {
                return Collections.emptyList();
            }
            String[] keys = splitCSV(lines.get(0));
            List<Map<String, String>> results = new ArrayList<Map<String, String>>();
            for (int l = 1; l < lines.size(); l ++) {
                final String line = lines.get(l);
                LOGGER.debug(line);
                String[] values = splitCSV(line);
                Map<String, String> m = new HashMap<String, String>();
                for (int i = 0; i < values.length; i ++) {
                    m.put(keys[i], values[i]);
                }
                results.add(m);
            }
            return results;
        } finally {
            get.releaseConnection();
        }

    }

    private static String CSV_SPLIT_PATTERN = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
    private String[] splitCSV(String line) {
        String[] result = line.split(CSV_SPLIT_PATTERN);
        for (int i = 0; i < result.length; i ++) {
            if (result[i].startsWith("\"")) {
                result[i] = result[i].substring(1, result[i].length() -1);
            }
        }
        return result;
    }

    private String getFusekiBaseUrl() {
        return fusekiBaseUrl;
    }
}
