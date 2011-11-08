package org.sakaiproject.nakamura.api.solr;

/**
 * An Indexer is a class that manages IndexHandler registrations against specific session
 * classes. This enables multiple IndexHandlers connecting to multiple content
 * Repositories potentially of different underlying types to produce documents to be
 * indexed.
 */
public interface Indexer {

  /**
   * Add a handler, against a key for a type of session.
   * 
   * @param key
   *          the key on which the handler should be selected, normally this is
   *          sling:resoureType 
   * @param handler
   *          the handler to be registered.
   */
  void addHandler(String key, IndexingHandler handler);

  /**
   * Remove the handler registration, if that registration exists.
   * 
   * @param key
   *          the key
   * @param handler
   *          the handler
   */
  void removeHandler(String key, IndexingHandler handler);

}
