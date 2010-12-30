package org.sakaiproject.nakamura.solr.handlers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.DateField;
import org.osgi.service.event.Event;
import org.sakaiproject.nakamura.api.solr.IndexingHandler;
import org.sakaiproject.nakamura.api.solr.RepositorySession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.jcr.AccessDeniedException;
import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

public class DefaultResourceTypeHandler implements IndexingHandler {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(DefaultResourceTypeHandler.class);
  private static final Set<String> WHITELIST_PROPERTIES = ImmutableSet.of("jcr:data");
  private static final Set<String> IGNORE_NAMESPACES = ImmutableSet.of("jcr", "rep");
  private static final Set<String> IGNORE_PROPERTIES = ImmutableSet.of();
  private static final Set<Integer> IGNORE_TYPE = ImmutableSet.of(PropertyType.NAME,
      PropertyType.PATH, PropertyType.REFERENCE, PropertyType.WEAKREFERENCE);
  private static final Map<String, String> INDEX_FIELD_MAP = ImmutableMap.of("jcr:data",
      "content");

  public Collection<SolrInputDocument> getDocuments(RepositorySession repositorySession, Event event) {
    LOGGER.debug("GetDocuments for {} ", event);
    String path = (String) event.getProperty("path");
    if ( ignorePath(path)) {
      return Collections.emptyList();
    }
    List<SolrInputDocument> documents = Lists.newArrayList();
    if (path != null) {
      try {
        Session session = repositorySession.adaptTo(Session.class);
        Node n = session.getNode(path);
        if (n != null) {
          String[] principals = getReadingPrincipals(n);
          SolrInputDocument doc = new SolrInputDocument();
          int nadd = 0;
          PropertyIterator pi = n.getProperties();
          LOGGER.debug("Workign from {} ", pi);
          while (pi.hasNext()) {
            Property p = pi.nextProperty();
            String name = p.getName();
            String indexName = index(p);
            if (indexName != null) {
              try {
                for (Object o : convertToIndex(p)) {
                  if (o != null) {
                    LOGGER.debug("Adding {} to index doc as {} ", name, o);
                    doc.addField(indexName, o);
                  } else {
                    LOGGER.debug("Skipping null value for {} ", name);
                  }
                }
              } catch (RepositoryException e) {
                LOGGER.error(e.getMessage(), e);
              }
            } else {
              LOGGER.debug("Ignoring {} ", name);
            }
          }
          LOGGER.debug("Added {} ", nadd);
          for (String principal : principals) {
            doc.addField("readers", principal);
          }
          doc.addField("id", path);
          documents.add(doc);
        }
      } catch (RepositoryException e) {
        LOGGER.error(e.getMessage(), e);
      }
    }
    return documents;
  }

  public Collection<String> getDeleteQueries(RepositorySession repositorySession, Event event) {
    LOGGER.debug("GetDelete for {} ", event);
    String path = (String) event.getProperty("path");
    boolean ignore = ignorePath(path);
    if ( ignore ) {
      return Collections.emptyList();
    } else {
      return ImmutableList.of("id:" + path);
    }
  }

  protected boolean ignorePath(String path) {
    if (path != null) {
      if ( path.contains("/rep:policy") ) {  
        return true;
      } else if ( path.contains("/jcr:content")) {
        return true;
      } else if ( path.startsWith("/jcr:system")) {
        return true;     
      }
    } else {
      return true;
    }
    return false;
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
                  if (Privilege.JCR_READ.equals(p)) {
                    matchesRead = true;
                    break;
                  } else {
                    for (Privilege ap : p.getAggregatePrivileges()) {
                      if (Privilege.JCR_READ.equals(ap)) {
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

  protected Iterable<Object> convertToIndex(Property p) throws RepositoryException {
    Value[] v = null;
    if (p.isMultiple()) {
      v = p.getValues();
    } else {
      v = new Value[] { p.getValue() };
    }
    LOGGER.debug("Value is {}", v);
    return new IterableWrapper<Object>(v) {

      @Override
      protected Object getValue(Object object) {
        try {
          if (object instanceof Value) {
            Value v = (Value) object;
            switch (v.getType()) {
            case PropertyType.BINARY:
              return v.getBinary().getStream();
            case PropertyType.BOOLEAN:
              return v.getBoolean();
            case PropertyType.DATE:
              return new DateField().toExternal(v.getDate().getTime());
            case PropertyType.DECIMAL:
              return v.getDecimal();
            case PropertyType.DOUBLE:
              return v.getDouble();
            case PropertyType.LONG:
              return v.getLong();
            case PropertyType.NAME:
              return v.getString();
            case PropertyType.PATH:
              return v.getString();
            case PropertyType.REFERENCE:
              return null;
            case PropertyType.STRING:
              return v.getString();
            case PropertyType.UNDEFINED:
              return v.getString();
            case PropertyType.URI:
              return v.getString();
            case PropertyType.WEAKREFERENCE:
              return null;
            default:
              return v.getString();
            }
          }
        } catch (Exception e) {
          LOGGER.error(e.getMessage(), e);
        }

        return null;
      }

    };
  }

  protected String index(Property p) throws RepositoryException {
    String name = p.getName();
    String[] parts = StringUtils.split(name, ':');
    if (!WHITELIST_PROPERTIES.contains(name)) {
      if (IGNORE_NAMESPACES.contains(parts[0])) {
        return null;
      }
      if (IGNORE_PROPERTIES.contains(name)) {
        return null;
      }
      int type = p.getType();
      if (IGNORE_TYPE.contains(type)) {
        return null;
      }
    }
    String mappedName = INDEX_FIELD_MAP.get(name);
    // only fields in the map will be used, and those are in the schema.
    return mappedName;
  }

}
