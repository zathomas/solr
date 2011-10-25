/*
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

import org.apache.solr.common.SolrInputDocument;
import org.osgi.service.event.Event;

import java.util.Collection;

/**
 *
 */
public interface ImmediateIndexingHandler {

  /**
   * Get a Collection of documents to be added to the index based on the event.
   * 
   * @param repositorySession
   *          an adaptable RepositorySession.
   * @param event
   *          the event that specifies the list of documents.
   * @return a collection of SolrInputDocuments to be indexed.
   */
  Collection<SolrInputDocument> getImmediateDocuments(RepositorySession repositorySession,
      Event event);

  /**
   * Get a collection of queries to execute as delete operations against the index in
   * response to an event. Delete operations are performed before add operations.
   * 
   * @param respositorySession
   *          an adaptable RepositorySession.
   * @param event
   *          the event that specified the delete queries.
   * @return a Collection of lucene queries to be executed as Solr Delete operations.
   */
  Collection<String> getImmediateDeleteQueries(RepositorySession respositorySession, Event event);
}
