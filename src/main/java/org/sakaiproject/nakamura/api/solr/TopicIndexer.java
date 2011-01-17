package org.sakaiproject.nakamura.api.solr;

/**
 * Allows registration of IndexHandlers against Topics. Implementations should expect
 * calls on the IndexHandler methods for all types of repository.
 */
public interface TopicIndexer {

  /**
   * Add a Topic based index handler.
   * 
   * @param topic
   *          the event topic that the IndexHandler will handle.
   * @param handler
   *          the handler being registered.
   */
  void addHandler(String topic, IndexingHandler handler);

  /**
   * Remove a handler.
   * 
   * @param topic
   *          the topic
   * @param handler
   *          the handler that was registered.
   */
  void removeHandler(String topic, IndexingHandler handler);

}
