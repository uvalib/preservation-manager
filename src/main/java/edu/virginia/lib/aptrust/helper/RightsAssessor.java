package edu.virginia.lib.aptrust.helper;

import org.apache.solr.common.SolrDocument;

public class RightsAssessor {

    public RightsStatement getRightsStatementForAvalonResource(SolrDocument solrRecord) {
        return RightsStatement.CC_ATTRIBUTION;
    }

}
