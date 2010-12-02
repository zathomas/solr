package org.sakaiproject.nakamura.api.solr;

import org.apache.solr.common.SolrInputDocument;
import org.osgi.service.event.Event;

import java.util.Collection;

import javax.jcr.Session;

public interface IndexingHandler {

  Collection<SolrInputDocument> getDocuments(Session session, Event event);

  Collection<String> getDeleteQueries(Session session, Event event);

}
