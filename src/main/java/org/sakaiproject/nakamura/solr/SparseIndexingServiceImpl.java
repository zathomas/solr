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
import org.apache.sling.commons.osgi.OsgiUtil;
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
import org.sakaiproject.nakamura.api.solr.ImmediateIndexingHandler;
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
    ImmediateIndexingHandler, ResourceIndexingService {

  private static final String PROP_TOPICS = "resource.topics";
  private static final Logger LOGGER = LoggerFactory
      .getLogger(SparseIndexingServiceImpl.class);
  // these are the names of system properites.
  private static final Set<String> SYSTEM_PROPERTIES = ImmutableSet.of(FIELD_ID, FIELD_READERS);

  @Reference
  protected TopicIndexer contentIndexer;
  private String[] topics;

  private Map<String, IndexingHandler> indexers = Maps.newConcurrentMap();
  private Map<String, ImmediateIndexingHandler> immediateIndexers = Maps.newConcurrentMap();
  private IndexingHandler defaultHandler;
  @SuppressWarnings("unchecked")
  private Map<String, String> ignoreCache = new LRUMap(500);
  @SuppressWarnings("unchecked")
  private Map<String, String> immediateIgnoreCache = new LRUMap(500);
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
    topics = OsgiUtil.toStringArray(properties.get(PROP_TOPICS), StoreListener.DEFAULT_TOPICS);
    for (String topic : topics) {
      contentIndexer.addImmediateHandler(topic, this);
      contentIndexer.addHandler(topic, this);
    }
  }

  @Deactivate
  public void deactivate(Map<String, Object> properties) {
    for (String topic : topics) {
      contentIndexer.removeImmediateHandler(topic, this);
      contentIndexer.removeHandler(topic, this);
    }
  }

  public Collection<SolrInputDocument> getImmediateDocuments(
      RepositorySession repositorySession, Event event) {
    String topic = event.getTopic();
    if (topic.endsWith(StoreListener.UPDATED_TOPIC) || topic.endsWith(StoreListener.ADDED_TOPIC)) {
      String path = (String) event.getProperty(FIELD_PATH);

      if (!ignore(path) && !immediateIgnoreCache.containsKey(path)) {
       ImmediateIndexingHandler handler = getHandler(repositorySession, path,
            this.immediateIndexers, this.immediateIgnoreCache);

        List<SolrInputDocument> outputDocs = Lists.newArrayList();
        Collection<SolrInputDocument> docs = handler.getImmediateDocuments(repositorySession, event);
        for (SolrInputDocument doc : docs) {
          // check the fields of the returned docs to make sure they contain atleast 1
          // field that is not a system property. this is not to filter out any system
          // properties but to make sure there are more things to index than just system
          // properties.
          if (!SYSTEM_PROPERTIES.containsAll(doc.getFieldNames())) {
            try {
              addDefaultFields(doc, repositorySession);
              outputDocs.add(doc);
            } catch (StorageClientException e) {
              LOGGER.warn("Failed to index {} cause: {} ", path, e.getMessage());
            }
          }
        }
        return outputDocs;
      } else {
        LOGGER.debug("Ignored action at path:{}  require on {} ", path, event);
      }
    } else {
      LOGGER.debug("No update action require on {} ", event);
    }
    return ImmutableList.of();
  }

  public Collection<SolrInputDocument> getDocuments(RepositorySession repositorySession,
      Event event) {
    String topic = event.getTopic();
    if (topic.endsWith(StoreListener.UPDATED_TOPIC) || topic.endsWith(StoreListener.ADDED_TOPIC)) {
      String path = (String) event.getProperty(FIELD_PATH);

      if (!ignore(path) && !ignoreCache.containsKey(path)) {
        IndexingHandler handler = getHandler(repositorySession, path, this.indexers, this.ignoreCache);

        List<SolrInputDocument> outputDocs = Lists.newArrayList();
        Collection<SolrInputDocument> docs = handler.getDocuments(repositorySession, event);
        for (SolrInputDocument doc : docs) {
          // check the fields of the returned docs to make sure they contain atleast 1
          // field that is not a system property. this is not to filter out any system
          // properties but to make sure there are more things to index than just system
          // properties.
          if (!SYSTEM_PROPERTIES.containsAll(doc.getFieldNames())) {
            try {
              addDefaultFields(doc, repositorySession);
              outputDocs.add(doc);
            } catch (StorageClientException e) {
              LOGGER.warn("Failed to index {} cause: {} ", path, e.getMessage());
            }
          }
          return outputDocs;
        }
      } else {
        LOGGER.debug("Ignored action at path:{}  require on {} ", path, event);
      }
    } else {
      LOGGER.debug("No update action require on {} ", event);
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
      // we don't overwrite the id field if it has been provided
      if (!doc.getFieldNames().contains(FIELD_ID)) {
        doc.setField(FIELD_ID, path);
      }
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

  @SuppressWarnings("unchecked")
  private <T> T getHandler(RepositorySession repositorySession, String path,
      Map<String, T> indexers, Map<String, String> ignoreCache) {
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
                T handler = indexers.get(resourceType);
                if (handler != null) {
                  LOGGER.debug("Handler of type {} found {} for {} from {} ", new Object[] {
                      resourceType, handler, path, indexers });
                  return handler;
                } else {
                  LOGGER.debug("Ignoring {}; no handler", path);
                  ignoreCache.put(path, path);
                }
              } else {
                LOGGER.debug("Ignored {} no resource type ",path);
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
    return (T) defaultHandler;
  }

  public Collection<String> getDeleteQueries(RepositorySession repositorySession,
      Event event) {
    String topic = event.getTopic();
    if (topic.endsWith(StoreListener.DELETE_TOPIC)) {
      String path = (String) event.getProperty(FIELD_PATH);
      if (!ignore(path) && !ignoreCache.containsKey(path)) {
        String resourceType = (String) event.getProperty("resourceType");

        IndexingHandler handler = null;
        if (resourceType != null) {
          handler = getHandler(resourceType, this.indexers);
        } else {
          handler = getHandler(repositorySession, path, this.indexers, this.ignoreCache);
        }
        return handler.getDeleteQueries(repositorySession, event);
      }
    } else {
      LOGGER.debug("No delete action require on {} ", event);
    }
    return ImmutableList.of();
  }

  public Collection<String> getImmediateDeleteQueries(RepositorySession repositorySession,
      Event event) {
    String topic = event.getTopic();
    if (topic.endsWith(StoreListener.DELETE_TOPIC)) {
      String path = (String) event.getProperty(FIELD_PATH);
      if (!ignore(path) && !immediateIgnoreCache.containsKey(path)) {
        String resourceType = (String) event.getProperty("resourceType");

        ImmediateIndexingHandler handler = null;
        if (resourceType != null) {
          handler = (ImmediateIndexingHandler) getHandler(resourceType,
              this.immediateIndexers);
        } else {
          handler = (ImmediateIndexingHandler) getHandler(repositorySession, path,
              this.immediateIndexers, this.immediateIgnoreCache);
        }
        return handler.getImmediateDeleteQueries(repositorySession, event);
      }
    } else {
      LOGGER.debug("No delete action require on {} ", event);
    }
    return ImmutableList.of();
  }

  @SuppressWarnings("unchecked")
  private <T> T getHandler(String resourceType, Map<String, T> indexers) {
    T handler = indexers.get(resourceType);
    if (handler == null) {
      handler = (T) defaultHandler;
    }
    return handler;
  }

  public void addHandler(String key, IndexingHandler handler) {
    LOGGER.debug("Added New Indexer as {} at {} ",  key,
        handler);
    indexers.put(key, handler);
    // reset what is ignored so the newly registered handler has a chance to respond to
    // any previously unhandled indexing
    ignoreCache.clear();
  }

  public void addImmediateHandler(String key, ImmediateIndexingHandler handler) {
    LOGGER.debug("Added New Immediate Indexer as {} at {} ",  key,
        handler);
    immediateIndexers.put(key, handler);
    // reset what is ignored so the newly registered handler has a chance to respond to
    // any previously unhandled indexing
    immediateIgnoreCache.clear();
  }

  public void removeHandler(String key, IndexingHandler handler) {
    if (handler.equals(indexers.get(key))) {
      indexers.remove(key);
    }
  }

  public void removeImmediateHandler(String key, ImmediateIndexingHandler handler) {
    if (handler.equals(immediateIndexers.get(key))) {
      immediateIndexers.remove(key);
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
