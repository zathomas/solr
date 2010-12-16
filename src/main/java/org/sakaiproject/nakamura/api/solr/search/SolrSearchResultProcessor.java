package org.sakaiproject.nakamura.api.solr.search;

import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.io.JSONWriter;

public interface SolrSearchResultProcessor {

  String DEFAULT_PROCESSOR_PROP = "sakai.solr.search.processor.default";

  SolrSearchResultSet getSearchResultSet(SlingHttpServletRequest request,
      String queryString) throws SolrSearchException;

  void writeResult(SlingHttpServletRequest request, JSONWriter write, Result result) throws JSONException;

}
