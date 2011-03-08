package org.sakaiproject.nakamura.solr;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.collections.map.LRUMap;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.solr.common.SolrInputDocument;
import org.osgi.service.event.Event;
import org.sakaiproject.nakamura.api.lite.Session;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.api.lite.StoreListener;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessControlManager;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.Permissions;
import org.sakaiproject.nakamura.api.lite.accesscontrol.Security;
import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.api.lite.content.ContentManager;
import org.sakaiproject.nakamura.api.solr.IndexingHandler;
import org.sakaiproject.nakamura.api.solr.RepositorySession;
import org.sakaiproject.nakamura.api.solr.ResourceIndexingService;
import org.sakaiproject.nakamura.api.solr.TopicIndexer;
import org.sakaiproject.nakamura.solr.handlers.DefaultSparseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component(immediate = true, metatype = true)
@Service(value = ResourceIndexingService.class)
@Properties( value={@Property(name="type", value="sparse" )})
public class SparseIndexingServiceImpl implements IndexingHandler,
    ResourceIndexingService {

  private static final String PROP_TOPICS = "resource.topics";
  private static final Logger LOGGER = LoggerFactory
      .getLogger(SparseIndexingServiceImpl.class);
  // these are the names of system properites.
  private static final Set<String> SYSTEM_PROPERTIES = ImmutableSet.of(FIELD_ID, FIELD_READERS);

  @Reference
  protected TopicIndexer contentIndexer;
  private String[] topics;

  private Map<String, IndexingHandler> indexers = Maps.newConcurrentHashMap();
  private IndexingHandler defaultHandler;
  @SuppressWarnings("unchecked")
  private Map<String, String> ignoreCache = new LRUMap(500);
  private static final String[] BLACK_LISTED = {
      "/dev/",
      "/devwidgets/",
      "/jsdoc/",
      "/dev/",
      "/var/",
      "/tests/",
      "/apps/"
  };

  @Activate
  public void activate(Map<String, Object> properties) {
    defaultHandler = new DefaultSparseHandler();
    topics = Utils.getSetting(properties.get(PROP_TOPICS), StoreListener.DEFAULT_TOPICS);
    for (String topic : topics) {
      contentIndexer.addHandler(topic, this);
    }
  }

  @Deactivate
  public void deactivate(Map<String, Object> properties) {
    for (String topic : topics) {
      contentIndexer.removeHandler(topic, this);
    }
  }

  public Collection<SolrInputDocument> getDocuments(RepositorySession repositorySession,
      Event event) {
    String topic = event.getTopic();
    if (topic.endsWith(StoreListener.UPDATED_TOPIC) || topic.endsWith(StoreListener.ADDED_TOPIC)) {
      String path = (String) event.getProperty(FIELD_PATH);
      if (!ignore(path)) {
        LOGGER.debug("Update action at path:{}  require on {} ", path, event);
        Collection<SolrInputDocument> docs = getHandler(repositorySession, path)
            .getDocuments(repositorySession, event);
        List<SolrInputDocument> outputDocs = Lists.newArrayList();
        for (SolrInputDocument doc : docs) {
          for (String name : doc.getFieldNames()) {
            // loop through the fields of the returned docs to make sure they contain
            // atleast 1 field that is not a system property. this is not to filter out
            // any system properties but to make sure there are more things to index than
            // just system properties.
            if (!SYSTEM_PROPERTIES.contains(name)) {
              try {
                addDefaultFields(doc, repositorySession);
                outputDocs.add(doc);
              } catch (StorageClientException e) {
                LOGGER.warn("Failed to index {} cause: {} ", path, e.getMessage());
              }
              break;
            }
          }
        }
        return outputDocs;
      } else {
        LOGGER.info("Ignored action at path:{}  require on {} ", path, event);
      }
    } else {
      LOGGER.info("No update action require on {} ", event);
    }
    return ImmutableList.of();
  }

  private void addDefaultFields(SolrInputDocument doc, RepositorySession repositorySession) throws StorageClientException {
    Object o = doc.getFieldValue(_DOC_SOURCE_OBJECT);
    if ( o instanceof Content ) {
      Content content = (Content) o;
      String[] principals = getReadingPrincipals(repositorySession, Security.ZONE_CONTENT, content.getPath());
      for (String principal : principals) {
        doc.addField(FIELD_READERS, principal);
      }
      if ( content.hasProperty("sling:resourceType")) {
        doc.setField(FIELD_RESOURCE_TYPE, content.getProperty("sling:resourceType"));
      }
      String path = content.getPath();
      doc.setField(FIELD_ID, path);
      while( path != null ) {
        doc.addField(FIELD_PATH, path);
        String newPath = Utils.getParentPath(path);
        if ( path.equals(newPath) ) {
          break;
        }
        path = newPath;
      }
      doc.removeField(_DOC_SOURCE_OBJECT);
    } else {
      LOGGER.error("Note to Developer: Indexer must add the _source fields so that the default fields can be set, please correct, SolrDoc was {} ",doc);
      throw new StorageClientException(_DOC_SOURCE_OBJECT+" fields was missing from Solr Document, please correct the handler implementation");

    }
  }


  private String[] getReadingPrincipals(RepositorySession repositorySession,
      String zone, String path) throws StorageClientException {
    Session session = repositorySession.adaptTo(Session.class);
    AccessControlManager accessControlManager = session.getAccessControlManager();
    return accessControlManager.findPrincipals(zone, path,Permissions.CAN_READ.getPermission(), true);
  }

  private IndexingHandler getHandler(RepositorySession repositorySession, String path) {
    org.sakaiproject.nakamura.api.lite.Session sparseSession = repositorySession
        .adaptTo(org.sakaiproject.nakamura.api.lite.Session.class);

    while (path != null) {
      if (!ignoreCache.containsKey(path)) {
        try {
          if (sparseSession != null) {
            ContentManager contentManager = sparseSession.getContentManager();
            Content c = contentManager.get(path);
            LOGGER.debug("Checking Content at {} got {} ", path, c);
            if (c != null) {
              if (c.hasProperty("sling:resourceType")) {
                String resourceType = (String) c.getProperty("sling:resourceType");
                IndexingHandler handler = indexers.get(resourceType);
                if (handler != null) {
                  LOGGER.debug("Handler of type {} found {} for {} from {} ", new Object[] {
                      resourceType, handler, path, indexers });
                  return handler;
                } else {
                  LOGGER.info("Ignored {} no handler for {} ", path, resourceType);
                  ignoreCache.put(path, path);
                }
              } else {
                LOGGER.info("Ignored {} no resource type ",path);
              }
            }
          }
        } catch (StorageClientException e) {
          LOGGER.debug(e.getMessage(), e);
        } catch (AccessDeniedException e) {
          LOGGER.debug(e.getMessage(), e);
        }
      }
      if ( StorageClientUtils.isRoot(path)) {
        break;
      }
      path = Utils.getParentPath(path);
    }
    return defaultHandler;
  }

  public Collection<String> getDeleteQueries(RepositorySession repositorySession,
      Event event) {
    String topic = event.getTopic();
    if (topic.endsWith(StoreListener.DELETE_TOPIC) || topic.endsWith(StoreListener.UPDATED_TOPIC)) {
      String path = (String) event.getProperty(FIELD_PATH);
      if (!ignore(path)) {
        String resourceType = (String) event.getProperty("resourceType");
        if (resourceType != null) {
          return getHandler(resourceType).getDeleteQueries(repositorySession,
              event);
        } else {
          return getHandler(repositorySession, path).getDeleteQueries(repositorySession,
              event);
        }
      }
    } else {
      LOGGER.debug("No delete action require on {} ", event);
    }
    return ImmutableList.of();
  }

  private IndexingHandler getHandler(String resourceType) {
    IndexingHandler handler = indexers.get(resourceType);
    if (handler == null) {
      handler = defaultHandler;
    }
    return handler;
  }

  public void addHandler(String key, IndexingHandler handler) {
    LOGGER.debug("Added New Indexer as {} at {} ",  key,
        handler);
    indexers.put( key, handler);
  }

  public void removeHandler(String key, IndexingHandler handler) {
    if (handler.equals(indexers.get(key))) {
      indexers.remove(key);
    }
  }

  private boolean ignore(String path) {
    if ( path == null ) {
      return true;
    }
    for ( String blackList : BLACK_LISTED ) {
      if ( path.startsWith(blackList)) {
        return true;
      }
    }
    return false;
  }

}
