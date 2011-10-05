package org.sakaiproject.nakamura.solr;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.io.FileUtils;
import org.apache.sling.jcr.api.SlingRepository;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.osgi.framework.BundleContext;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.event.Event;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.solr.SolrClient;
import org.sakaiproject.nakamura.lite.BaseMemoryRepository;
import org.sakaiproject.nakamura.solr.handlers.FileResourceTypeHandler;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.Dictionary;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;
import javax.jcr.nodetype.NodeType;
import javax.xml.parsers.ParserConfigurationException;

public class AddContentToEmbeddedSolrTest {

  @Mock
  private ComponentContext componentContext;
  @Mock
  private BundleContext bundleContext;
  @Mock
  private SlingRepository repository;
  @Mock
  private Session session;
  @Mock
  private ConfigurationAdmin configurationAdmin;
  @Mock
  private Configuration configuration;

  private EmbeddedSolrClient embeddedSolrClient;
  private SolrServerServiceImpl solrServerServiceImpl;
  private ContentEventListener contentEventListener;
  private BaseMemoryRepository sparseRepository;

  public AddContentToEmbeddedSolrTest() {
    MockitoAnnotations.initMocks(this);
  }

  public void startEmbeddedSolr() throws IOException, ParserConfigurationException,
      SAXException, ClientPoolException, StorageClientException, AccessDeniedException, ClassNotFoundException {
    embeddedSolrClient = new EmbeddedSolrClient();
    Mockito.when(componentContext.getBundleContext()).thenReturn(bundleContext);
    FileUtils.deleteQuietly(new File("target/slingtest"));
    Mockito.when(bundleContext.getProperty("sling.home")).thenReturn("target/slingtest");
    Dictionary<String, Object> properties = new Hashtable<String, Object>();
    Mockito.when(componentContext.getProperties()).thenReturn(properties);
    embeddedSolrClient.configurationAdmin = configurationAdmin;
    Mockito.when(configurationAdmin.getConfiguration(EmbeddedSolrClient.class.getName()))
        .thenReturn(null);
    Mockito.when(
        configurationAdmin.createFactoryConfiguration(
            "org.apache.sling.commons.log.LogManager.factory.config", null)).thenReturn(
        configuration);
    embeddedSolrClient.activate(componentContext);    
    solrServerServiceImpl = new SolrServerServiceImpl();
    solrServerServiceImpl.bind(embeddedSolrClient);
    solrServerServiceImpl.activate(ImmutableMap.of(SolrClient.CLIENT_NAME, (Object)SolrClient.EMBEDDED));
    sparseRepository = new BaseMemoryRepository();
  }

  public void stopEmbeddedSolr() throws IOException, ParserConfigurationException,
      SAXException {
    sparseRepository.close();
    embeddedSolrClient.deactivate(componentContext);
  }

  public void startContentListener() throws RepositoryException, IOException, ClientPoolException, StorageClientException, AccessDeniedException {
    contentEventListener = new ContentEventListener();
    contentEventListener.repository = repository;
    contentEventListener.sparseRepository = sparseRepository.getRepository();
    contentEventListener.solrServerService = solrServerServiceImpl;
    Map<String, Object> properties = new HashMap<String, Object>();
    contentEventListener.activate(properties);
  }

  public void stopContentListener() throws RepositoryException, IOException,
      InterruptedException {
    Map<String, Object> properties = new HashMap<String, Object>();
    contentEventListener.deactivate(properties);
    contentEventListener.getQueueDispatcher().join();
  }

  private void bindContentNode(String path, String nodeTypeName, String resourceType,
      boolean withContentNode) throws RepositoryException {

    Node contentNode = Mockito.mock(Node.class);
    Mockito.when(session.getNode(path)).thenReturn(contentNode);

    NodeType nodeType = Mockito.mock(NodeType.class);
    Mockito.when(contentNode.getPrimaryNodeType()).thenReturn(nodeType);
    Mockito.when(nodeType.getName()).thenReturn(nodeTypeName);
    if (resourceType != null) {
      Mockito.when(contentNode.hasProperty("sling:resourceType")).thenReturn(true);
      Property resourceTypeProperty = Mockito.mock(Property.class);
      Mockito.when(contentNode.getProperty("sling:resourceType")).thenReturn(
          resourceTypeProperty);
      Mockito.when(resourceTypeProperty.getString()).thenReturn(resourceType);
    }
    if (withContentNode) {
      Mockito.when(contentNode.hasNode(Node.JCR_CONTENT)).thenReturn(true);
      Node cnode = Mockito.mock(Node.class);
      Mockito.when(contentNode.getNode(Node.JCR_CONTENT)).thenReturn(cnode);
      PropertyIterator pi = addPropertyIterator(cnode);
      Mockito.when(pi.hasNext()).thenReturn(true, true, true, false);
      GregorianCalendar c = new GregorianCalendar();
      c.setTime(new Date());
      Property p1 = createProperty("jcr:data", PropertyType.BINARY,
          "The Quick Brown fox jumped over the fence to run away from the wolf.");
      Property p2 = createProperty("lastModified", PropertyType.DATE, c);
      Property p3 = createProperty("lastModifiedBy", PropertyType.STRING, "me");
      Mockito.when(pi.nextProperty()).thenReturn(p1, p2, p3);

    }

    PropertyIterator pi = addPropertyIterator(contentNode);
    Mockito.when(pi.hasNext()).thenReturn(true, true, true, false);
    Property p1 = createProperty("jcr:path", PropertyType.STRING, "/path/to/content");
    Property p2 = createProperty("sakai:group", PropertyType.STRING, "a group Name");
    Property p3 = createProperty("someother", PropertyType.STRING, "green", "grow",
        "the", "therushes");
    Mockito.when(pi.nextProperty()).thenReturn(p1, p2, p3);
  }

  private Property createProperty(String name, int type, Object... values)
      throws RepositoryException {
    Property jcrProperty = Mockito.mock(Property.class);
    Mockito.when(jcrProperty.getName()).thenReturn(name);
    Mockito.when(jcrProperty.getType()).thenReturn(type);
    if (values.length > 1) {
      Mockito.when(jcrProperty.isMultiple()).thenReturn(true);
      Value[] v = new Value[values.length];
      for (int i = 0; i < values.length; i++) {
        v[i] = createValue(type, values[i]);

      }
      Mockito.when(jcrProperty.getValues()).thenReturn(v);
    } else {
      Mockito.when(jcrProperty.isMultiple()).thenReturn(false);
      Value v = createValue(type, values[0]);
      Mockito.when(jcrProperty.getValue()).thenReturn(v);
    }
    return jcrProperty;
  }

  private Value createValue(int type, Object object) throws ValueFormatException,
      IllegalStateException, RepositoryException {
    Value v = Mockito.mock(Value.class);
    Mockito.when(v.getType()).thenReturn(type);
    switch (type) {
    case PropertyType.BINARY:
      Binary b = Mockito.mock(Binary.class);
      Mockito.when(v.getBinary()).thenReturn(b);
      Mockito.when(b.getStream()).thenReturn(
          new ByteArrayInputStream(((String) object).getBytes()));
      return v;
    case PropertyType.BOOLEAN:
      Mockito.when(v.getBoolean()).thenReturn((Boolean) object);
      return v;
    case PropertyType.DATE:
      Mockito.when(v.getDate()).thenReturn((Calendar) object);
      return v;
    case PropertyType.DECIMAL:
      Mockito.when(v.getDecimal()).thenReturn((BigDecimal) object);
      return v;
    case PropertyType.DOUBLE:
      Mockito.when(v.getDouble()).thenReturn((Double) object);
      return v;
    case PropertyType.LONG:
      Mockito.when(v.getLong()).thenReturn((Long) object);
      return v;
    case PropertyType.NAME:
      Mockito.when(v.getString()).thenReturn((String) object);
      return v;
    case PropertyType.PATH:
      Mockito.when(v.getString()).thenReturn((String) object);
      return v;
    case PropertyType.REFERENCE:
      return v;
    case PropertyType.STRING:
      Mockito.when(v.getString()).thenReturn((String) object);
      return v;
    case PropertyType.UNDEFINED:
      Mockito.when(v.getString()).thenReturn((String) object);
      return v;
    case PropertyType.URI:
      Mockito.when(v.getString()).thenReturn((String) object);
      return v;
    case PropertyType.WEAKREFERENCE:
      return v;
    default:
      Mockito.when(v.getString()).thenReturn((String) object);
      return v;
    }
  }

  private PropertyIterator addPropertyIterator(Node contentNode)
      throws RepositoryException {
    PropertyIterator contentNodePropertyIterator = Mockito.mock(PropertyIterator.class);
    Mockito.when(contentNode.getProperties()).thenReturn(contentNodePropertyIterator);
    return contentNodePropertyIterator;
  }

  @Test
  public void addContentTest() throws IOException, ParserConfigurationException,
      SAXException, RepositoryException, InterruptedException, ClientPoolException, StorageClientException, AccessDeniedException, ClassNotFoundException {
    Mockito.when(repository.loginAdministrative(null)).thenReturn(session);
    startEmbeddedSolr();
    startContentListener();

    ResourceIndexingServiceImpl resourceIndexingServiceImpl = new ResourceIndexingServiceImpl();
    resourceIndexingServiceImpl.contentIndexer = contentEventListener;
    resourceIndexingServiceImpl.activate(ImmutableMap.of("test", new Object()));

    FileResourceTypeHandler fileResourceTypeHandler = new FileResourceTypeHandler();
    fileResourceTypeHandler.setResourceIndexingService(resourceIndexingServiceImpl);
    fileResourceTypeHandler.activate(ImmutableMap.of("test", new Object()));

    bindContentNode("/a/new/bit/of/content", "nt:unstructured", null, false);
    bindContentNode("/a/new/bit/of/profile", "nt:unstructured", "sakai:group-profile",
        false);
    bindContentNode("/a/new/bit/of/file", "nt:file", null, true);

    Dictionary<String, Object> properties = new Hashtable<String, Object>();
    properties.put("path", "/a/new/bit/of/content");
    contentEventListener.handleEvent(new Event(
        "org/apache/sling/api/resource/Resource/CREATED", properties));
    contentEventListener.handleEvent(new Event(
        "org/apache/sling/api/resource/Resource/REMOVED", properties));
    properties.put("path", "/a/new/bit/of/profile");
    contentEventListener.handleEvent(new Event(
        "org/apache/sling/api/resource/Resource/CREATED", properties));
    properties.put("path", "/a/new/bit/of/file");
    contentEventListener.handleEvent(new Event(
        "org/apache/sling/api/resource/Resource/CREATED", properties));

    Thread.sleep(1000);

    stopContentListener();

    fileResourceTypeHandler.deactivate(ImmutableMap.of("test", new Object()));
    resourceIndexingServiceImpl.deactivate(ImmutableMap.of("test", new Object()));
    contentEventListener.getQueueDispatcher().join();

    stopEmbeddedSolr();

    Mockito.validateMockitoUsage();

  }

}
