package edu.virginia.lib.aptrust.helper;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
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
        List<Map<String, String>> response = getQueryResponse(query);
        if (response.size() > 1) {
            throw new RuntimeException("More than one record mached query! " + query);
        } else if (response.size() < 1) {
            return Collections.emptyMap();
        }
        return response.get(0);
    }

    public List<Map<String, String>> getQueryResponse(final String query) throws IOException {
        final String queryUrl = getFusekiBaseUrl() + "/query?query=" + URLEncoder.encode(query, "UTF-8") +
                "&default-graph-uri=&output=csv&stylesheet=";
        HttpGet get = new HttpGet(queryUrl);
        try {
            HttpResponse r = client.execute(get);
            Reader in = new InputStreamReader(r.getEntity().getContent());
            
            List<Map<String, String>> results = new ArrayList<Map<String, String>>();
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);
            for (CSVRecord record : records) {
                Map<String, String> m = new HashMap<String, String>();
                results.add(record.toMap());
            }
            return results;
        } finally {
            get.releaseConnection();
        }

    }

    private String getFusekiBaseUrl() {
        return fusekiBaseUrl;
    }
}
