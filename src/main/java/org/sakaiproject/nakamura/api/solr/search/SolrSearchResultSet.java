package org.sakaiproject.nakamura.api.solr.search;

import java.util.Iterator;


public interface SolrSearchResultSet {

  Iterator<Result> getResultSetIterator();

  long getSize();

}
