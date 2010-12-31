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
   *          sling:resoureType or in the case of JCR it might also be the primary node
   *          type.
   * @param handler
   *          the handler to be registered.
   * @param sessionClass
   *          The session type required for this registration. The Indexer will inspect
   *          the event and determine the Repository from which it came. It will then
   *          select only IndexHandlers that are capable of Handling that type of
   *          repository based on the Session class for that repository. So if the Event
   *          identifies a JCR Node, then only IndexHandlers registered with
   *          javax.jcr.Session will be selected. Similarly if the Event identifies a
   *          Sparse Content item then on Index Handlers registered against Sparse
   *          Sessions will be selected. If the IndexHandler is capable to handling more
   *          than one type of Session, then it should be registered with each session
   *          type it can handle.
   */
  void addHandler(String key, IndexingHandler handler, Class<?> sessionClass);

  /**
   * Remove the handler registration, if that registration exists.
   * 
   * @param key
   *          the key
   * @param handler
   *          the handler
   * @param sessionClass
   *          the session class that the registration was made against.
   */
  void removeHander(String key, IndexingHandler handler, Class<?> sessionClass);

}
