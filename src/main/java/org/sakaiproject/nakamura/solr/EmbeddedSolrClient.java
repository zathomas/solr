package org.sakaiproject.nakamura.solr;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;
import org.sakaiproject.nakamura.api.solr.SolrServerService;
import org.xml.sax.SAXException;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

@Component
public class EmbeddedSolrClient implements SolrServerService {

  private EmbeddedSolrServer server;
  private String solrHome;
  private CoreContainer coreContainer;

  @Activate
  public void activate(ComponentContext componentContext) throws IOException, ParserConfigurationException, SAXException {
    BundleContext bundleContext = componentContext.getBundleContext();
    solrHome = Utils.getSolrHome(bundleContext);
    // Note that the following property could be set through JVM level arguments too
    System.setProperty("solr.solr.home", solrHome);
    CoreContainer.Initializer initializer = new CoreContainer.Initializer();
    coreContainer = initializer.initialize();
    server = new EmbeddedSolrServer(coreContainer, "");
    
  }
  
  @Deactivate
  public void deactivate(ComponentContext componentContext) {
    coreContainer.shutdown();
  }
  
  public SolrServer getServer() {
    return server;
  }
  
  public String getSolrHome() {
    return solrHome;
  }
  
 
}
