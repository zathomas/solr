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
