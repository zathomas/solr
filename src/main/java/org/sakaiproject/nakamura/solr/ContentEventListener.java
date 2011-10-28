package org.sakaiproject.nakamura.solr;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.jcr.RepositoryException;

import org.apache.commons.lang.StringUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.felix.scr.annotations.Services;
import org.apache.sling.jcr.api.SlingRepository;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;
import org.osgi.service.event.EventConstants;
import org.osgi.service.event.EventHandler;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.Repository;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.solr.IndexingHandler;
import org.sakaiproject.nakamura.api.solr.SolrServerService;
import org.sakaiproject.nakamura.api.solr.TopicIndexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Manages one or more queues storing events on disk so that those events can be
 * replayed later against the solr index.
 * 
 * @author ieb
 * 
 */
@Component(immediate = true, metatype = true)
@Services(value = { @Service(value = EventHandler.class),
		@Service(value = TopicIndexer.class) })
public class ContentEventListener implements EventHandler, TopicIndexer,
		QueueManagerDriver {


	@Property(value = { "org/sakaiproject/nakamura/lite/*",
			"org/apache/sling/api/resource/Resource/*" }, propertyPrivate = true)
	static final String TOPICS = EventConstants.EVENT_TOPIC;

	/**
	 * Near real time enables soft commit to the index. You must also lower the
	 * TTL, and tune the batch size to eliminate delays. If this is enabled all
	 * documents are added to the index with a soft commit and will be available
	 * in the index immediately. Documents still go through the queue to avoid
	 * all the threading issues that crippled Sakai 2 search updates, but the
	 * dwell time in the queue is greatly reduced. Be warned, that if running in
	 * a cluster extra work will have to be done to ensure that soft commits are
	 * propagated appropriately throughout the cluster. Also you will need to
	 * enable soft commits in the solrconfig.xml for NRT to work... which is why
	 * its disabled by default.
	 */
	private static final String PROP_NEAR_REAL_TIME = "near-real-time";


	private static final Logger LOGGER = LoggerFactory
			.getLogger(ContentEventListener.class);

	private static final Object QUEUE_NAME = "name";

	private static final String DEFAULT_QUEUE_NAME = "";

	private static final int DEFAULT_BATCHED_INDEX_SIZE = 100;

	private static final long DEFAULT_BATCH_DELAY = 5000;

	private static final boolean DEFAULT_NEAR_REAL_TIME = false;


	private static final String BATCHED_INDEX_SIZE = "batched-index-size";

	private static final String BATCH_DELAY = "batch-delay";

	@Property(value={
			"name=;batch-delay=5000;batched-index-size=100;near-real-time=false",
			"name=high;batch-delay=50;batched-index-size=10;near-real-time=true"
	})
	private static final String PROP_QUEUE_CONFIG = "queue-config";

	private static final String[] QUEUE_DEFAULT = {
		"name=;batch-delay=5000;batched-index-size=100;near-real-time=false",
		"name=high;batch-delay=50;batched-index-size=10;near-real-time=true"
	};

	@Reference
	protected SolrServerService solrServerService;

	@Reference
	protected SlingRepository repository;

	@Reference
	protected Repository sparseRepository;

	@Reference
	protected EventAdmin eventAdmin;

	private Map<String, Collection<IndexingHandler>> handlers = Maps
			.newConcurrentMap();

	/**
	 * A protective lock surrounding adding and removing keys from the handlers
	 * map. THis is there because we could have 2 threads adding to the same key
	 * at the same time. Its not there to protect the map itself or access to
	 * iterators on the objects in the map as those changes are still atomic.
	 * see usage for detail.
	 */
	private Object handlersLock = new Object();

	/**
	 * The queues orders smallest ttl first.
	 */
	private QueueManager[] queues = null;

	@Activate
	protected void activate(Map<String, Object> properties)
			throws RepositoryException, IOException, ClientPoolException,
			StorageClientException, AccessDeniedException {
		String[] queuesConfig = Utils.toStringArray(properties.get(PROP_QUEUE_CONFIG),
				QUEUE_DEFAULT);
		Map<String, QueueManager> qm = Maps.newHashMap();
		for (String queueConfig : queuesConfig) {
			String[] props = StringUtils.split(queueConfig, ";");
			Builder<String, String> b = ImmutableMap.builder();
			for (String kv : props) {
				String[] p = StringUtils.split(kv, "=", 2);
				if (p.length == 1) {
					b.put(p[0],"");
				} else if ( p.length == 2) {
					b.put(p[0], p[1]);
				} else {
					throw new IllegalArgumentException("Invalid configuraton "
							+ queueConfig + " " + kv);
				}
			}
			Map<String, String> config = b.build();
			boolean nearRealTime = Utils.toBoolean(config.get(PROP_NEAR_REAL_TIME),
					DEFAULT_NEAR_REAL_TIME);
			int batchedIndexSize = Utils.toInt(config.get(BATCHED_INDEX_SIZE),
					DEFAULT_BATCHED_INDEX_SIZE);

			long batchDelay = Utils.toLong(config.get(BATCH_DELAY),
					DEFAULT_BATCH_DELAY);
			String name = Utils.toString(config.get(QUEUE_NAME), DEFAULT_QUEUE_NAME);
			qm.put(name, new QueueManager(this,
					solrServerService.getSolrHome(), name, nearRealTime,
					batchedIndexSize, batchDelay));

		}
		List<QueueManager> qmlist = Lists.newArrayList(qm.values());
		Collections.sort(qmlist, new Comparator<QueueManager>() {
			public int compare(QueueManager o1, QueueManager o2) {
				return (int) (o1.batchDelay - o2.batchDelay);
			}
		});
		queues = qmlist.toArray(new QueueManager[qmlist.size()]);

		startAll();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				stopAll();
			}
		});

	}

	@Deactivate
	protected void deactivate(Map<String, Object> properties)
			throws IOException {
		stopAll();
	}

	/**
	 * Handles an event from OSGi and places it in the appropriate queue.
	 * @see org.osgi.service.event.EventHandler#handleEvent(org.osgi.service.event.Event)
	 */
	public void handleEvent(Event event) {
		String topic = event.getTopic();
		LOGGER.debug("Got Event {} {} ", event, handlers);
		Collection<IndexingHandler> contentIndexHandler = handlers.get(topic);
		if (contentIndexHandler != null && contentIndexHandler.size() > 0) {
			try {
				int ttl = Utils.toInt(event.getProperty(TopicIndexer.TTL),
						Integer.MAX_VALUE);
				QueueManager q = null;
				for (QueueManager qm : queues) {
					if (ttl < qm.batchDelay) {
						if (q == null) {
							LOGGER.info("Unable to satisfy TTL of {} on event {} ",
									ttl, event);
						} else {
							q.saveEvent(event);
							q = null;
							break;
						}
					}
					q = qm;
				}
				if (q != null) {
					q.saveEvent(event);
				}
			} catch (IOException e) {
				LOGGER.warn(e.getMessage(), e);
			}
		}
	}

	public void addHandler(String topic, IndexingHandler handler) {
		synchronized (handlersLock) {
			Collection<IndexingHandler> topicHandlers = handlers.get(topic);
			if (topicHandlers == null) {
				topicHandlers = Sets.newHashSet();
			} else {
				// make a copy to avoid concurrency issues in the topicHandler
				topicHandlers = Sets.newHashSet(topicHandlers);
			}
			topicHandlers.add(handler);
			handlers.put(topic, topicHandlers);
		}
	}

	public void removeHandler(String topic, IndexingHandler handler) {
		synchronized (handlersLock) {
			Collection<IndexingHandler> topicHandlers = handlers.get(topic);
			if (topicHandlers != null && topicHandlers.size() > 0) {
				topicHandlers = Sets.newHashSet(topicHandlers);
				topicHandlers.remove(handler);
				handlers.put(topic, topicHandlers);
			}
		}
	}

	public Collection<IndexingHandler> getTopicHandler(String topic) {
		return handlers.get(topic);
	}

	public SolrServerService getSolrServerService() {
		return solrServerService;
	}

	public EventAdmin getEventAdmin() {
		return eventAdmin;
	}

	public SlingRepository getSlingRepository() {
		return repository;
	}

	public Repository getSparseRepository() {
		return sparseRepository;
	}

	public void startAll() {
		if (queues != null) {
			for (QueueManager q : queues) {
				q.start();
			}
		}
	}

	public void stopAll() {
		if (queues != null) {
			for (QueueManager q : queues) {
				try {
					q.stop();
				} catch (Exception e) {

				}
			}
		}
		queues = null;
	}

	// used only for testing
	protected void joinAll() throws InterruptedException {
		if (queues != null) {
			for (QueueManager q : queues) {
				q.getQueueDispatcher().join();
			}
		}
	}

	// used only for testing
	public void closeAll() throws IOException {
		if (queues != null) {
			for (QueueManager q : queues) {
				q.closeWriter();
			}
		}
	}


}
