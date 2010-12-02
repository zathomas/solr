package org.sakaiproject.nakamura.api.solr;


public interface Indexer {

  void removeHander(String key, IndexingHandler handler);

  void addHandler(String key, IndexingHandler handler);

}
