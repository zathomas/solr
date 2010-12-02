package org.sakaiproject.nakamura.solr.handlers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.osgi.service.event.Event;
import org.sakaiproject.nakamura.api.solr.IndexingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;

public class DefaultResourceTypeHandler implements IndexingHandler {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(DefaultResourceTypeHandler.class);
  private static final Set<String> WHITELIST_PROPERTIES = ImmutableSet.of("jcr:data");
  private static final Set<String> IGNORE_NAMESPACES = ImmutableSet.of("jcr", "rep");
  private static final Set<String> IGNORE_PROPERTIES = ImmutableSet.of();
  private static final Set<Integer> IGNORE_TYPE = ImmutableSet.of(PropertyType.NAME,
      PropertyType.PATH, PropertyType.REFERENCE, PropertyType.WEAKREFERENCE);
  private static final Map<String,String> INDEX_FIELD_MAP = ImmutableMap.of(
      "jcr:data", "content"
      );

  public Collection<SolrInputDocument> getDocuments(Session session, Event event) {
    LOGGER.info("GetDocuments for {} ", event);
    String path = (String) event.getProperty("path");
    List<SolrInputDocument> documents = Lists.newArrayList();
    if (path != null) {
      try {
        Node n = session.getNode(path);
        if (n != null) {
          String[] principals = getReadingPrincipals(n);
          SolrInputDocument doc = new SolrInputDocument();
          int nadd = 0;
          PropertyIterator pi = n.getProperties();
          LOGGER.info("Workign from {} ", pi);
          while (pi.hasNext()) {
            Property p = pi.nextProperty();
            String name = p.getName();
            String indexName = index(p);
            if (indexName != null) {
              try {
                for (Object o : convertToIndex(p)) {
                  if (o != null) {
                    LOGGER.info("Adding {} to index doc as {} ", name, o);
                    doc.addField(indexName, o);
                  } else {
                    LOGGER.info("Skipping null value for {} ", name);
                  }
                }
              } catch (RepositoryException e) {
                LOGGER.error(e.getMessage(), e);
              }
            } else {
              LOGGER.info("Ignoring {} ", name);
            }
            nadd++;
          }
          LOGGER.info("Added {} ", nadd);
          for (String principal : principals) {
            doc.addField("solr.readers", principal);
          }
          doc.addField("solr.id", path);
          documents.add(doc);
        }
      } catch (RepositoryException e) {
        LOGGER.error(e.getMessage(), e);
      }
    }
    return documents;
  }

  public Collection<String> getDeleteQueries(Session session, Event event) {
    LOGGER.info("GetDelete for {} ", event);
    String path = (String) event.getProperty("path");
    if (path != null) {
      return ImmutableList.of("solr.is:" + path);
    } else {
      return Collections.emptyList();
    }
  }

  private String[] getReadingPrincipals(Node n) {
    LOGGER
        .warn("Not applying read principals at this time, FIXME before deleting this log message");
    // TODO Auto-generated method stub
    return new String[0];
  }

  protected Iterable<Object> convertToIndex(Property p) throws RepositoryException {
    Value[] v = null;
    if (p.isMultiple()) {
      v = p.getValues();
    } else {
      v = new Value[] { p.getValue() };
    }
    LOGGER.info("Value is {}",v);
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
              return v.getDate();
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
    if ( mappedName == null ) {
      mappedName = name;
    }
    return mappedName;
  }

}
