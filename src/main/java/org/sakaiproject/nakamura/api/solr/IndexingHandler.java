package org.sakaiproject.nakamura.api.solr;

import org.apache.solr.common.SolrInputDocument;
import org.osgi.service.event.Event;

import java.util.Collection;

public interface IndexingHandler {

  Collection<SolrInputDocument> getDocuments(RepositorySession repositorySession, Event event);

  Collection<String> getDeleteQueries(RepositorySession respositorySession, Event event);

}
