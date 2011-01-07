package org.sakaiproject.nakamura.solr;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.collections.map.LRUMap;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.api.SlingConstants;
import org.apache.solr.common.SolrInputDocument;
import org.osgi.service.event.Event;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.api.lite.content.ContentManager;
import org.sakaiproject.nakamura.api.solr.IndexingHandler;
import org.sakaiproject.nakamura.api.solr.RepositorySession;
import org.sakaiproject.nakamura.api.solr.ResourceIndexingService;
import org.sakaiproject.nakamura.api.solr.TopicIndexer;
import org.sakaiproject.nakamura.solr.handlers.DefaultResourceTypeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

@Component(immediate = true, metatype = true)
@Service(value = ResourceIndexingService.class)
public class ResourceIndexingServiceImpl implements IndexingHandler,
    ResourceIndexingService {

  private static final String PROP_TOPICS = "resource.topics";
  private static final String REMOVE_TOPIC = "REMOVED";
  private static final String ADDED_TOPIC = "ADDED";
  private static final String CHANGED_TOPIC = "CHANGED";
  private static final String[] DEFAULT_TOPICS = { SlingConstants.TOPIC_RESOURCE_ADDED,
      SlingConstants.TOPIC_RESOURCE_CHANGED, SlingConstants.TOPIC_RESOURCE_REMOVED };
  private static final Logger LOGGER = LoggerFactory
      .getLogger(ResourceIndexingServiceImpl.class);
  // these are the names of system properites.
  private static final Set<String> SYSTEM_PROPERTIES = ImmutableSet.of("id", "readers");

  @Reference
  protected TopicIndexer contentIndexer;
  private String[] topics;

  private Map<String, IndexingHandler> indexers = Maps.newConcurrentHashMap();
  private IndexingHandler defaultHandler;
  @SuppressWarnings("unchecked")
  private Map<String, String> ignoreCache = new LRUMap(500);

  @Activate
  public void activate(Map<String, Object> properties) {
    defaultHandler = new DefaultResourceTypeHandler();
    topics = Utils.getSetting(properties.get(PROP_TOPICS), DEFAULT_TOPICS);
    for (String topic : topics) {
      contentIndexer.addHandler(topic, this);
    }
  }

  @Deactivate
  public void deactivate(Map<String, Object> properties) {
    for (String topic : topics) {
      contentIndexer.removeHander(topic, this);
    }
  }

  public Collection<SolrInputDocument> getDocuments(RepositorySession repositorySession,
      Event event) {
    String topic = event.getTopic();
    if (topic.endsWith(CHANGED_TOPIC) || topic.endsWith(ADDED_TOPIC)) {
      String path = (String) event.getProperty("path");
      LOGGER.debug("Update action at path:{}  require on {} ", path, event);
      if (path != null) {
        Collection<SolrInputDocument> docs = getHander(repositorySession, path)
            .getDocuments(repositorySession, event);
        List<SolrInputDocument> outputDocs = Lists.newArrayList();
        for (SolrInputDocument doc : docs) {
          for (String name : doc.getFieldNames()) {
            if (!SYSTEM_PROPERTIES.contains(name)) {
              outputDocs.add(doc);
              break;
            }
          }
        }
        return outputDocs;
      }
    } else {
      LOGGER.debug("No update action require on {} ", event);
    }
    return ImmutableList.of();
  }

  private IndexingHandler getHander(RepositorySession repositorySession, String path) {
    Session session = repositorySession.adaptTo(Session.class);
    org.sakaiproject.nakamura.api.lite.Session sparseSession = repositorySession
        .adaptTo(org.sakaiproject.nakamura.api.lite.Session.class);
    LOGGER.info("getHandler at JCR Session {}  Sparse Session {} ", session,
        sparseSession);

    while (path != null && !"/".equals(path)) {
      if (!ignoreCache.containsKey(path)) {
        try {
          if (session != null) {

            Node n = session.getNode(path);
            LOGGER.info("Checking for Node at {} found {} ", path, n);
            if (n != null) {
              String resourceType = session.getClass().getName() + ":"
                  + n.getPrimaryNodeType().getName();
              if (n.hasProperty("sling:resourceType")) {
                resourceType = session.getClass().getName() + ":"
                    + n.getProperty("sling:resourceType").getString();
              }
              IndexingHandler handler = indexers.get(resourceType);
              LOGGER.info("Handler of type {} found {} from {} ", new Object[] {
                  resourceType, handler, indexers });
              if (handler != null) {
                return handler;
              } else {
                ignoreCache.put(path, path);
              }
            }
          }
        } catch (RepositoryException e) {
          LOGGER.debug(e.getMessage(), e);
        }
        try {
          if (sparseSession != null) {
            ContentManager contentManager = sparseSession.getContentManager();
            Content c = contentManager.get(path);
            LOGGER.info("Checking Content at {} got {} ", path, c);
            if (c != null) {
              if (c.hasProperty("sling:resourceType")) {
                String resourceType = sparseSession.getClass().getName() + ":"
                    + StorageClientUtils.toString(c.getProperty("sling:resourceType"));
                IndexingHandler handler = indexers.get(resourceType);
                LOGGER.info("Handler of type {} found {} from {} ", new Object[] {
                    resourceType, handler, indexers });
                if (handler != null) {
                  return handler;
                } else {
                  ignoreCache.put(path, path);
                }
              }
            }
          }
        } catch (StorageClientException e) {
          LOGGER.debug(e.getMessage(), e);
        } catch (AccessDeniedException e) {
          LOGGER.debug(e.getMessage(), e);
        }
      }
      path = Utils.getParentPath(path);
    }
    return defaultHandler;
  }

  public Collection<String> getDeleteQueries(RepositorySession repositorySession,
      Event event) {
    String topic = event.getTopic();
    if (topic.endsWith(REMOVE_TOPIC) || topic.endsWith(CHANGED_TOPIC)) {
      String path = (String) event.getProperty("path");
      if (path != null) {
        return getHander(repositorySession, path).getDeleteQueries(repositorySession,
            event);
      }
    } else {
      LOGGER.debug("No delete action require on {} ", event);
    }
    return ImmutableList.of();
  }

  public void addHandler(String key, IndexingHandler handler, Class<?> sessionClass) {
    LOGGER.debug("Added New Indexer as {} at {} ", sessionClass.getName() + ":" + key,
        handler);
    indexers.put(sessionClass.getName() + ":" + key, handler);
  }

  public void removeHander(String key, IndexingHandler handler, Class<?> sessionClass) {
    if (handler.equals(indexers.get(sessionClass.getName() + ":" + key))) {
      indexers.remove(sessionClass.getName() + ":" + key);
    }
  }

}
