package org.sakaiproject.nakamura.solr;

import junit.framework.Assert;

import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;

public class SolrRemoteClientTest {


  @Mock
  private ComponentContext componentContext;
  @Mock
  private BundleContext bundleContext;
  
  public SolrRemoteClientTest() {
   MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testRemoteSolrClient() throws IOException {
    RemoteSolrClient remoteSolrClient = new RemoteSolrClient();
    Mockito.when(componentContext.getBundleContext()).thenReturn(bundleContext);
    Mockito.when(bundleContext.getProperty("sling.home")).thenReturn("target/slingtest");
    Dictionary<String, Object> properties = new Hashtable<String, Object>();
    Mockito.when(componentContext.getProperties()).thenReturn(properties);
    remoteSolrClient.activate(componentContext);
    Assert.assertNotNull(remoteSolrClient.getSolrHome());
    Assert.assertNotNull(remoteSolrClient.getServer());
    remoteSolrClient.deactivate(componentContext);
  }
}
