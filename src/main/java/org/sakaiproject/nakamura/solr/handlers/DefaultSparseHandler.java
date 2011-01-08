package org.sakaiproject.nakamura.solr.handlers;

import org.apache.solr.common.SolrInputDocument;
import org.osgi.service.event.Event;
import org.sakaiproject.nakamura.api.solr.IndexingHandler;
import org.sakaiproject.nakamura.api.solr.RepositorySession;

import java.util.Collection;
import java.util.Collections;

public class DefaultSparseHandler implements IndexingHandler {

  public Collection<SolrInputDocument> getDocuments(RepositorySession repositorySession,
      Event event) {
    return Collections.emptyList();
  }

  public Collection<String> getDeleteQueries(RepositorySession respositorySession,
      Event event) {
    return Collections.emptyList();
  }

}
