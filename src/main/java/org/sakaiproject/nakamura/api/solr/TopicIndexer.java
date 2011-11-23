/**
 * Licensed to the Sakai Foundation (SF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The SF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
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
