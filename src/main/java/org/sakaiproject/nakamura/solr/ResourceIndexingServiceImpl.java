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
import org.apache.sling.api.SlingConstants;
import org.apache.solr.common.SolrInputDocument;
import org.osgi.service.event.Event;
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
import java.util.Map.Entry;
import java.util.Set;

import javax.jcr.AccessDeniedException;
import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

@Component(immediate = true, metatype = true)
@Service(value = ResourceIndexingService.class)
@Properties( value={@Property(name="type", value="jcr" )})
public class ResourceIndexingServiceImpl implements IndexingHandler,
    ResourceIndexingService {

  private static final String PROP_TOPICS = "resource.topics";
  private static final String REMOVE_TOPIC = "REMOVED";
  private static final String ADDED_TOPIC = "ADDED";
  private static final String CHANGED_TOPIC = "CHANGED";
  private static final String[] DEFAULT_TOPICS = { SlingConstants.TOPIC_RESOURCE_ADDED,
      SlingConstants.TOPIC_RESOURCE_CHANGED, SlingConstants.TOPIC_RESOURCE_REMOVED,  };
  private static final Logger LOGGER = LoggerFactory
      .getLogger(ResourceIndexingServiceImpl.class);
  // these are the names of system properites.
  private static final Set<String> SYSTEM_PROPERTIES = ImmutableSet.of("id", FIELD_READERS);

  @Reference
  protected TopicIndexer contentIndexer;
  private String[] topics;

  private Map<String, IndexingHandler> indexers = Maps.newConcurrentHashMap();
  private IndexingHandler defaultHandler;
  @SuppressWarnings("unchecked")
  private Map<String, String> ignoreCache = new LRUMap(500);
  private String lastRoot;
  private int lastRootN;
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
    defaultHandler = new DefaultResourceTypeHandler();
    topics = Utils.getSetting(properties.get(PROP_TOPICS), DEFAULT_TOPICS);
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
    if (topic.endsWith(CHANGED_TOPIC) || topic.endsWith(ADDED_TOPIC)) {
      String path = (String) event.getProperty("path");
      LOGGER.debug("Update action at path:{}  require on {} ", path, event);
      
      if (!ignore(path)) {
        Collection<SolrInputDocument> docs = getHandler(repositorySession, path)
            .getDocuments(repositorySession, event);
        List<SolrInputDocument> outputDocs = Lists.newArrayList();
        for (SolrInputDocument doc : docs) {
          for (String name : doc.getFieldNames()) {
            if (!SYSTEM_PROPERTIES.contains(name)) {
              try {
                addDefaultFields(doc);
                outputDocs.add(doc);
              } catch (RepositoryException e) {
                LOGGER.error("Failed to index {} cause: {} ",path, e.getMessage());
              }
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
  
  private void addDefaultFields(SolrInputDocument doc) throws RepositoryException {
    Node node = (Node) doc.getFieldValue(_DOC_SOURCE_OBJECT);
    if ( node != null ) {
      String[] principals = getReadingPrincipals(node);
      for (String principal : principals) {
        doc.addField(FIELD_READERS, principal);
      }
      String resourceType = node.getPrimaryNodeType().getName();
      if ( node.hasProperty("sling:resourceType")) {
        resourceType = node.getProperty("sling:resourceType").getString();
      }
      doc.addField(FIELD_RESOURCE_TYPE, resourceType);
      String path = node.getPath();
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
      throw new RepositoryException(_DOC_SOURCE_OBJECT+" fields was missing from Solr Document, please correct the handler implementation");
    } 
  }


  private IndexingHandler getHandler(RepositorySession repositorySession, String path) {
    Session session = repositorySession.adaptTo(Session.class);

    while (!isRoot(path)) {
      if (!ignoreCache.containsKey(path)) {
        try {
          if (session != null) {

            Node n = session.getNode(path);
            LOGGER.debug("Checking for Node at {} found {} ", path, n);
            if (n != null) {
              String resourceType = n.getPrimaryNodeType().getName();
              if (n.hasProperty("sling:resourceType")) {
                resourceType = n.getProperty("sling:resourceType").getString();
              }
              IndexingHandler handler = indexers.get(resourceType);
              if (handler != null) {
                LOGGER.debug("Handler of type {} found {} for {} from {} ", new Object[] {
                    resourceType, handler, path, indexers });
                return handler;
              } else {
                LOGGER.debug("Ignoring {} type {} no handler ", path, resourceType);
                ignoreCache.put(path, path);
              }
            }
          }
        } catch (RepositoryException e) {
          LOGGER.debug(e.getMessage(), e);
        }
      }
      path = Utils.getParentPath(path);
    }
    return defaultHandler;
  }

  /**
   * @param path
   * @return true if the paths is a root, that includes "", "/" "something". Non root paths contain a /
   */
  private boolean isRoot(String path) {
    if ( path == null || "/".equals(path) || path.length() == 0 ) {
      lastRoot = path;
      lastRootN = 0;
      return true;
    }
    if ( path.indexOf("/") >= 0 ) {
      if ( path.equals(lastRoot) ) {
        lastRootN++;
      } else {
        lastRoot = path;
        lastRootN = 0;
      }
      if ( lastRootN > 1000) {
        // must be an infinite loop here
        LOGGER.info("Looks like an infinite loop on {}, please fix this method, isRoot() ",lastRoot);
        lastRoot = path;
        lastRootN = 0;
        return true;
      }
      return false;
    }
    
    lastRoot = path;
    lastRootN = 0;
    return true;
  }

  public Collection<String> getDeleteQueries(RepositorySession repositorySession,
      Event event) {
    String topic = event.getTopic();
    if (topic.endsWith(REMOVE_TOPIC)) {
      String path = (String) event.getProperty("path");
      if (!ignore(path)) {
        return getHandler(repositorySession, path).getDeleteQueries(repositorySession,
            event);
      }
    } else {
      LOGGER.debug("No delete action require on {} ", event);
    }
    return ImmutableList.of();
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

  

/**
 * Gets the principals that can read the node. Principals are stored as protected nodes.
 * If the node has a mix:accessControllable then it will have a sub node rep:policy
 * which will have one or more nodes of type rep:GrantACE or rep:DenyACE each of those
 * nodes has a principal name and privileges.
 * 
 * At the node we need to build a set of all principals that have the read bit set by
 * looking at the current node and then looking at its parent and so on
 * 
 * @param n
 * @return
 */
private String[] getReadingPrincipals(Node n) {
  List<String> principals = Lists.newArrayList();
  try {
    Map<String, Boolean> readPrincipals = Maps.newHashMap();
    Session session = n.getSession();
    AccessControlManager acm = session.getAccessControlManager();
    while (n != null ) {
      if (n.hasNode("rep:policy")) {
        try {
          Node policy = n.getNode("rep:policy");
          for (NodeIterator ni = policy.getNodes(); ni.hasNext();) {
            Node ace = ni.nextNode();
            try {
              String principal = ace.getProperty("rep:principalName").getString();
              Value[] privilegeNames = ace.getProperty("rep:privileges").getValues();
              boolean matchesRead = false;
              for (Value privilegeName : privilegeNames) {
                Privilege p = acm.privilegeFromName(privilegeName.getString());
                if (Privilege.JCR_READ.equals(p.getName())) {
                  matchesRead = true;
                  break;
                } else {
                  for (Privilege ap : p.getAggregatePrivileges()) {
                    if (Privilege.JCR_READ.equals(ap.getName())) {
                      matchesRead = true;
                      break;
                    }
                  }
                  if (matchesRead) {
                    break;
                  }
                }
              }

              Boolean canRead = readPrincipals.get(principal);
              if (matchesRead) {
                if (!canRead) {
                  // already denied
                  continue;
                } else if (canRead) {
                  // has been granted already
                  if ("rep:GrantACE".equals(ace.getPrimaryNodeType().getName())) {
                  } else {
                    // deny
                    readPrincipals.put(principal, Boolean.FALSE);
                  }
                } else {
                  if ("rep:GrantACE".equals(ace.getPrimaryNodeType().getName())) {
                    // deny
                    readPrincipals.put(principal, Boolean.TRUE);
                  } else {
                    // deny
                    readPrincipals.put(principal, Boolean.FALSE);
                  }
                }
              }
            } catch (RepositoryException e) {
              LOGGER.info("Ignoring ace node {} cause: {}", ace.getPath(),
                  e.getMessage());
            }
          }
        } catch (RepositoryException e) {
          LOGGER.info("Ignoring policy node on {} cause: {}", n.getPath(),
              e.getMessage());
        }
      }
      try {
        if ( "/".equals(n.getPath()) ) {
          break;
        }
        n = n.getParent();
      } catch (AccessDeniedException e) {
        LOGGER.info("Failed to get Parent node  {} cause: {}", n.getPath(),
            e.getMessage());
        break;
      } catch (ItemNotFoundException e) {
        LOGGER.info("Failed to get Parent node  {} cause: {}", n.getPath(),
            e.getMessage());
        break;
      } catch (RepositoryException e) {
        LOGGER.info("Failed to get Parent node  {} cause: {}", n.getPath(),
            e.getMessage());
        break;
      }
    }
    for (Entry<String, Boolean> readPrincipal : readPrincipals.entrySet()) {
      if (readPrincipal.getValue()) {
        principals.add(readPrincipal.getKey());
      }
    }
  } catch (RepositoryException e) {
    LOGGER.info("Failed to process ACLs on {} cause: {}", n, e.getMessage());
  }
  return principals.toArray(new String[principals.size()]);

}



}
