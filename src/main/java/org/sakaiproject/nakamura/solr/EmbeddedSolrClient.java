package org.sakaiproject.nakamura.solr;

import org.apache.commons.io.IOUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NakamuraSolrConfig;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.osgi.framework.BundleContext;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.ComponentContext;
import org.sakaiproject.nakamura.api.solr.SolrServerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LocationAwareLogger;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Dictionary;
import java.util.Hashtable;

import javax.xml.parsers.ParserConfigurationException;

@Component(immediate = true, metatype = true)
@Service(value = SolrServerService.class)
public class EmbeddedSolrClient implements SolrServerService {

  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedSolrClient.class);
  /**
   * According to the doc, this is thread safe and must be shared between all threads.
   */
  private EmbeddedSolrServer server;
  private String solrHome;
  private CoreContainer coreContainer;
  private SolrCore nakamuraCore;

  @Reference
  protected ConfigurationAdmin configurationAdmin;

  @Activate
  public void activate(ComponentContext componentContext) throws IOException,
      ParserConfigurationException, SAXException {
    BundleContext bundleContext = componentContext.getBundleContext();
    solrHome = Utils.getSolrHome(bundleContext);
    // Note that the following property could be set through JVM level arguments too
    LOGGER
        .info(" ================================== Creating new Logger for Embedded Solr");
    Configuration logConfiguration = configurationAdmin.createFactoryConfiguration(
        "org.apache.sling.commons.log.LogManager.factory.config", null);

    Dictionary<String, Object> loggingProperties = new Hashtable<String, Object>();
    loggingProperties.put("pid", this.getClass().getName());
    loggingProperties.put("org.apache.sling.commons.log.level", "INFO");
    loggingProperties.put("org.apache.sling.commons.log.file", "logs/solr.log");
    loggingProperties.put("org.apache.sling.commons.log.names", "org.apache.solr");
    logConfiguration.update(loggingProperties);

    System.setProperty("solr.solr.home", solrHome);
    File solrHomeFile = new File(solrHome);
    File coreDir = new File(solrHomeFile, "nakamura");
    // File coreConfigDir = new File(solrHomeFile,"conf");
    deployFile(solrHomeFile, "solr.xml");
    // deployFile(coreConfigDir,"solrconfig.xml");
    // deployFile(coreConfigDir,"schema.xml");
    ClassLoader contextClassloader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
    try {
      NakamuraSolrResourceLoader loader = new NakamuraSolrResourceLoader(solrHome, this
          .getClass().getClassLoader());
      coreContainer = new CoreContainer(loader);
      SolrConfig config = new NakamuraSolrConfig(loader, "solrconfig.xml",
          getStream("solrconfig.xml"));
      IndexSchema schema = new IndexSchema(config, null, getStream("schema.xml"));
      nakamuraCore = new SolrCore("nakamura", coreDir.getAbsolutePath(), config, schema,
          null);
      coreContainer.register("nakamura", nakamuraCore, false);
      server = new EmbeddedSolrServer(coreContainer, "nakamura");
      LoggerFactory.getLogger(this.getClass()).info("Contans cores {} ",
          coreContainer.getCoreNames());
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassloader);
    }

  }

  private InputStream getStream(String name) {
    return this.getClass().getResourceAsStream(name);
  }

  private void deployFile(File destDir, String target) throws IOException {
    if (!destDir.isDirectory()) {
      destDir.mkdirs();
    }
    File destFile = new File(destDir, target);
    if (!destFile.exists()) {
      InputStream in = Utils.class.getClassLoader().getResourceAsStream(target);
      OutputStream out = new FileOutputStream(destFile);
      IOUtils.copy(in, out);
      out.close();
      in.close();
    }
  }

  @Deactivate
  public void deactivate(ComponentContext componentContext) {
    nakamuraCore.close();
    coreContainer.shutdown();
  }

  public SolrServer getServer() {
    return server;
  }

  public String getSolrHome() {
    return solrHome;
  }

}
