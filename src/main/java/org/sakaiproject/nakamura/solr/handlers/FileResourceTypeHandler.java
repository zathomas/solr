package org.sakaiproject.nakamura.solr.handlers;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.solr.common.SolrInputDocument;
import org.osgi.service.event.Event;
import org.sakaiproject.nakamura.api.solr.ResourceIndexingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

@Component
public class FileResourceTypeHandler extends DefaultResourceTypeHandler {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(FileResourceTypeHandler.class);
  @Reference
  protected ResourceIndexingService resourceIndexingService;

  @Activate
  public void activate(Map<String, Object> properties) {
    resourceIndexingService.addHandler("nt:file", this);
  }

  @Deactivate
  public void deactivate(Map<String, Object> properties) {
    resourceIndexingService.removeHander("nt:file", this);
  }

  public Collection<SolrInputDocument> getDocuments(Session session, Event event) {
    Collection<SolrInputDocument> docs = super.getDocuments(session, event);
    for (SolrInputDocument d : docs) {
      String id = (String) d.getFieldValue("id");
      LOGGER.info("Adding File Information to  {} ", id);
      try {
        Node n = session.getNode(id);
        if (n.hasNode(Node.JCR_CONTENT)) {
          Node content = n.getNode(Node.JCR_CONTENT);
          if (content != null) {
            LOGGER.info("Loading Content");
            for (PropertyIterator pi = content.getProperties(); pi.hasNext();) {
              Property p = pi.nextProperty();
              String mappedName = index(p);
              if (mappedName != null) {
                for (Object o : convertToIndex(p)) {
                  LOGGER.info("Storing {} as {} {}",new Object[]{p.getName(),mappedName,o});
                  d.addField(mappedName, o);
                }
              }
            }
          } else {
            LOGGER.info("No Content Node found");
          }
        } else {
          LOGGER.info("Content Node not present");
        }
      } catch (RepositoryException e) {
        LOGGER.error(e.getMessage(), e);
      }

    }
    return docs;
  }

  public Collection<String> getDeleteQueries(Session session, Event event) {
    return super.getDeleteQueries(session, event);
  }

  public void setResourceIndexingService(ResourceIndexingService resourceIndexingService) {
    if (resourceIndexingService != null) {
      this.resourceIndexingService = resourceIndexingService;
    }
  }
}
