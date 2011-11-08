package org.sakaiproject.nakamura.solr;

import java.util.Collection;

import org.osgi.service.event.EventAdmin;
import org.sakaiproject.nakamura.api.lite.Repository;
import org.sakaiproject.nakamura.api.solr.IndexingHandler;
import org.sakaiproject.nakamura.api.solr.SolrServerService;

/**
 * Interface for the queue manager driver, mainly to avoid passing everything in
 * and to make it easier to unit test the QueueManager in isolation.
 * 
 * @author ieb
 * 
 */
interface QueueManagerDriver {

	Collection<IndexingHandler> getTopicHandler(String topic);

	SolrServerService getSolrServerService();

	EventAdmin getEventAdmin();

	Repository getSparseRepository();

}
