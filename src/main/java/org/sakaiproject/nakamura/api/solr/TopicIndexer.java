package org.sakaiproject.nakamura.api.solr;

/**
 * Allows registration of IndexHandlers against Topics. Implementations should
 * expect calls on the IndexHandler methods for all types of repository.
 */
public interface TopicIndexer {

	/**
	 * If this property is set on the Event causing the indexing operation the
	 * Indexer implementation will make best efforts to ensure that the item is
	 * indexed and appears in the search results before the TTL expires. Please
	 * use this sparingly as the more items that have to be indexed immediately
	 * the more resource will be require to deliver that TTL in production.
	 */
	public static final String TTL = "index-ttl";

	/**
	 * Add a Topic based index handler.
	 * 
	 * @param topic
	 *            the event topic that the IndexHandler will handle.
	 * @param handler
	 *            the handler being registered.
	 */
	void addHandler(String topic, IndexingHandler handler);

	/**
	 * Remove a handler.
	 * 
	 * @param topic
	 *            the topic
	 * @param handler
	 *            the handler that was registered.
	 */
	void removeHandler(String topic, IndexingHandler handler);

}
