package org.sakaiproject.nakamura.solr;

import org.apache.sling.jcr.api.SlingRepository;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.osgi.service.event.Event;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.solr.IndexingHandler;
import org.sakaiproject.nakamura.api.solr.RepositorySession;
import org.sakaiproject.nakamura.api.solr.SolrServerService;
import org.sakaiproject.nakamura.lite.BaseMemoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.xml.parsers.ParserConfigurationException;

public class ContentEventListenerTest {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ContentEventListener.class);
  @Mock
  private SlingRepository repository;
  @Mock
  private Session session;
  
  @Mock
  private SolrServerService solrServerService;
  @Mock
  private SolrServer server;
  private BaseMemoryRepository baseMemoryRepository;

  public ContentEventListenerTest() throws IOException, ParserConfigurationException,
      SAXException, ClientPoolException, StorageClientException, AccessDeniedException, ClassNotFoundException {
    MockitoAnnotations.initMocks(this);
    baseMemoryRepository = new BaseMemoryRepository();
  }

  @Test
  public void testContentEventListener() throws IOException, RepositoryException,
      InterruptedException, ClientPoolException, StorageClientException, AccessDeniedException {
    final ContentEventListener contentEventListener = new ContentEventListener();
    contentEventListener.sparseRepository = baseMemoryRepository.getRepository();
    contentEventListener.repository = repository;

    contentEventListener.solrServerService = solrServerService;
    Map<String, Object> properties = new HashMap<String, Object>();
    Mockito.when(solrServerService.getSolrHome()).thenReturn("target/solrtest");
    Mockito.when(solrServerService.getServer()).thenReturn(server);
    
    Mockito.when(repository.loginAdministrative(null)).thenReturn(session);
    contentEventListener.activate(properties);

    IndexingHandler h =  new IndexingHandler() {
      
      public Collection<SolrInputDocument> getDocuments(RepositorySession repositorySession, Event event) {
        return new ArrayList<SolrInputDocument>();
      }
      
      public Collection<String> getDeleteQueries(RepositorySession repositorySession, Event event) {
        return new ArrayList<String>();
      }

    };
    contentEventListener.addHandler("test/topic",h);
    for (int j = 0; j < 10; j++) {
      LOGGER.info("Adding Events ");
      for (int i = 0; i < 100; i++) {
        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put("evnumber", i);
        props.put("a nasty,key", "with a, nasty \n value");
        contentEventListener.handleEvent(new Event("test/topic", props));
      }
      Thread.sleep(100);
    }
    contentEventListener.closeWriter();
    LOGGER.info("Done adding Events ");

    contentEventListener.removeHandler("/test/topic", h);

    contentEventListener.deactivate(properties);

    LOGGER.info("Waiting for worker thread ");
    contentEventListener.getQueueDispatcher().join();
    LOGGER.info("Joined worker thread");
  }

}
