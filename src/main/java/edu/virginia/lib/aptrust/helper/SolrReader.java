package edu.virginia.lib.aptrust.helper;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.ModifiableSolrParams;

import java.net.MalformedURLException;
import java.util.Iterator;

public class SolrReader {

    private SolrServer solr;

    public SolrReader(String url) throws MalformedURLException {
        solr = new HttpSolrServer(url);
    }

    public Iterator<SolrDocument> getRecordsForQuery(String query) throws SolrServerException {
        int start = 0;
        final ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", new String[] { query });
        p.set("rows", 100);
        p.set("start", start);
        return new Iterator<SolrDocument>() {

            int index = 0;
            int start = 0;
            QueryResponse response = null;

            public boolean hasNext() {
                if (response == null || response.getResults().size() <= index) {
                    p.set("rows", 100);
                    p.set("start", start);
                    try {
                        response = solr.query(p);
                        start += response.getResults().size();
                        index = 0;
                    } catch (SolrServerException e) {
                        throw new RuntimeException(e);
                    }
                }
                return response.getResults().size() > index;
            }

            public SolrDocument next() {
                if (!hasNext()) {
                    throw new IllegalStateException();
                }
                return response.getResults().get(index ++);
            }
        };
    }
}
