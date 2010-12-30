package org.sakaiproject.nakamura.api.solr;


public interface TopicIndexer {

  void addHandler(String topic, IndexingHandler handler);

  void removeHander(String topic, IndexingHandler handler);

}
