package edu.virginia.lib.aptrust.helper;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;

public class APTrustAPIHelper {

    public static final String APPLICATION_JSON = "application/json";
    
    private String baseUrl;
    
    private String apiKey;
    
    private String user;
    
    public APTrustAPIHelper(final String baseUrl, final String apiKey, final String user) {
        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
        this.user = user;
    }
    
    /**
     * Gets a Json Object with information about an APTrust ingest operation for
     * the item with the given URI and etag, or returns null if none is available.
     * The result format is simply the serialized json from the AP Trust api.
     * @throws IOException 
     * @throws IllegalStateException 
     */
    public JsonObject getIngestReportForResource(String name, String etag) throws IOException {
        final String url = baseUrl + "/items?action=Ingest&per_page=50&name_exact=" + URLEncoder.encode(name, "UTF-8");
        HttpGet get = new HttpGet(url);
        
        get.addHeader("Accept", APPLICATION_JSON);
        get.addHeader("X-Fluctus-API-User", user);
        get.addHeader("X-Fluctus-API-Key", apiKey);
        
        try {
            final HttpResponse response = HttpHelper.createClient().execute(get);
            if (response.getStatusLine().getStatusCode() < 200 || response.getStatusLine().getStatusCode() >= 300) {
                throw new RuntimeException(response.getStatusLine() + " result from request to get " + url);
            }
            JsonReader reader = Json.createReader(new InputStreamReader(response.getEntity().getContent()));
            JsonObject results = reader.readObject();
            if (results.get("next") == null) {
                throw new RuntimeException("more than one page of results!");
            }
            JsonArray resultsArray = results.getJsonArray("results");
            for (int i = 0; i < resultsArray.size(); i ++) {
                final JsonObject result = resultsArray.getJsonObject(i);
                if (result.getString("etag").equals(etag)) {
                    return result;
                }
            }
            return null;
        } finally {
            get.releaseConnection();
        }
        
    }
    
}
