package org.sakaiproject.nakamura.api.solr.search;

import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.commons.json.io.JSONWriter;

import java.util.Iterator;

public interface SolrSearchBatchResultProcessor {

  String DEFAULT_BATCH_PROCESSOR_PROP = "sakai.search.processor.batch.default";


  void writeResults(SlingHttpServletRequest request, JSONWriter write,
      Iterator<Result> iterator);

  SolrSearchResultSet getSearchResultSet(SlingHttpServletRequest request,
      String queryString);

}
