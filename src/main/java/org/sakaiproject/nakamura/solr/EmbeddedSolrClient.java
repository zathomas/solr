package org.sakaiproject.nakamura.solr;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Dictionary;
import java.util.Hashtable;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.NakamuraSolrConfig;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.ComponentContext;
import org.sakaiproject.nakamura.api.solr.SolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

@Component(immediate = true, metatype = true)
@Service(value = SolrClient.class)
public class EmbeddedSolrClient implements SolrClient {

	@Property(value = SolrClient.EMBEDDED)
	public static final String CLIENT_NAME = SolrClient.CLIENT_NAME;

	private static final Logger LOGGER = LoggerFactory
			.getLogger(EmbeddedSolrClient.class);

	private static final String LOGGER_KEY = "org.sakaiproject.nakamura.logger";
	private static final String LOGGER_VAL = "org.apache.solr";
	/**
	 * According to the doc, this is thread safe and must be shared between all
	 * threads.
	 */
	private EmbeddedSolrServer server;
	private String solrHome;
	private CoreContainer coreContainer;
	private SolrCore nakamuraCore;

	@Property(value = "solrconfig.xml")
	private static final String PROP_SOLR_CONFIG = "solrconfig";
	@Property(value = "schema.xml")
	private static final String PROP_SOLR_SCHEMA = "solrschema";

	@Reference
	protected ConfigurationAdmin configurationAdmin;

	private boolean enabled;

	private Dictionary<String, Object> properties;

	private SolrClientListener listener;

	@SuppressWarnings("unchecked")
	@Activate
	public void activate(ComponentContext componentContext) throws IOException,
			ParserConfigurationException, SAXException {
		BundleContext bundleContext = componentContext.getBundleContext();
		solrHome = Utils.getSolrHome(bundleContext);
		properties = componentContext.getProperties();
	}

	public void enable(SolrClientListener listener) throws IOException,
			ParserConfigurationException, SAXException {
		if (enabled) {
			return;
		}
		String schemaLocation = Utils.toString(
				properties.get(PROP_SOLR_SCHEMA), "schema.xml");
		String configLocation = Utils.toString(
				properties.get(PROP_SOLR_CONFIG), "solrconfig.xml");
		// Note that the following property could be set through JVM level
		// arguments too
		LOGGER.debug("Logger for Embedded Solr is in {slinghome}/log/solr.log at level INFO");
		Configuration logConfiguration = getLogConfiguration();

		// create a log configuration if none was found. leave alone any found
		// configurations
		// so that modifications will persist between server restarts
		if (logConfiguration == null) {
			logConfiguration = configurationAdmin.createFactoryConfiguration(
					"org.apache.sling.commons.log.LogManager.factory.config",
					null);
			Dictionary<String, Object> loggingProperties = new Hashtable<String, Object>();
			loggingProperties.put("org.apache.sling.commons.log.level", "INFO");
			loggingProperties.put("org.apache.sling.commons.log.file",
					"logs/solr.log");
			loggingProperties.put("org.apache.sling.commons.log.names",
					"org.apache.solr");
			// add this property to give us something unique to re-find this
			// configuration
			loggingProperties.put(LOGGER_KEY, LOGGER_VAL);
			logConfiguration.update(loggingProperties);
		}

		System.setProperty("solr.solr.home", solrHome);
		File solrHomeFile = new File(solrHome);
		File coreDir = new File(solrHomeFile, "nakamura");
		// File coreConfigDir = new File(solrHomeFile,"conf");
		deployFile(solrHomeFile, "solr.xml");
		// deployFile(coreConfigDir,"solrconfig.xml");
		// deployFile(coreConfigDir,"schema.xml");
		ClassLoader contextClassloader = Thread.currentThread()
				.getContextClassLoader();
		Thread.currentThread().setContextClassLoader(
				this.getClass().getClassLoader());
		ClosableInputSource schemaSource = null;
		ClosableInputSource configSource = null;
		try {
			NakamuraSolrResourceLoader loader = new NakamuraSolrResourceLoader(
					solrHome, this.getClass().getClassLoader());
			coreContainer = new CoreContainer(loader);
			configSource = getSource(configLocation);
			schemaSource = getSource(schemaLocation);
			LOGGER.info("Configuring with Config {} schema {} ",
					configLocation, schemaLocation);
			SolrConfig config = new NakamuraSolrConfig(loader, configLocation,
					configSource);
			IndexSchema schema = new IndexSchema(config, schemaLocation,
					schemaSource);
			CoreDescriptor coreDescriptor = new CoreDescriptor(coreContainer,
					"nakamura", coreDir.getAbsolutePath() + "nakamura");
			nakamuraCore = new SolrCore("nakamura", coreDir.getAbsolutePath(),
					config, schema, coreDescriptor);
			coreContainer.register("nakamura", nakamuraCore, false);
			server = new EmbeddedSolrServer(coreContainer, "nakamura");
			LoggerFactory.getLogger(this.getClass()).info("Contans cores {} ",
					coreContainer.getCoreNames());
			this.enabled = true;
			this.listener = listener;

		} finally {
			Thread.currentThread().setContextClassLoader(contextClassloader);
			safeClose(schemaSource);
			safeClose(configSource);
		}

	}

	private void safeClose(ClosableInputSource source) {
		if (source != null) {
			try {
				source.close();
			} catch (IOException e) {
				LOGGER.debug(e.getMessage(), e);
			}
		}
	}

	private Configuration getLogConfiguration() throws IOException {
		Configuration logConfiguration = null;
		try {
			Configuration[] configs = configurationAdmin.listConfigurations("("
					+ LOGGER_KEY + "=" + LOGGER_VAL + ")");
			if (configs != null && configs.length > 0) {
				logConfiguration = configs[0];
			}
		} catch (InvalidSyntaxException e) {
			// ignore this as we'll create what we need
		}
		return logConfiguration;
	}

	private ClosableInputSource getSource(String name) throws IOException {
		if (name.contains(":")) {
			// try a URL
			try {
				URL u = new URL(name);
				InputStream in = u.openStream();
				if (in != null) {

					return new ClosableInputSource(in);
				}
			} catch (IOException e) {
				LOGGER.debug(e.getMessage(), e);
			}
		}
		// try a file
		File f = new File(name);
		if (f.exists()) {
			return new ClosableInputSource(new FileInputStream(f));
		} else {
			// try classpath
			InputStream in = this.getClass().getClassLoader()
					.getResourceAsStream(name);
			if (in == null) {
				LOGGER.error(
						"Failed to locate stream {}, tried URL, filesystem ",
						name);
				throw new IOException("Failed to locate stream " + name
						+ ", tried URL, filesystem ");
			}
			return new ClosableInputSource(in);
		}
	}

	private void deployFile(File destDir, String target) throws IOException {
		if (!destDir.isDirectory()) {
			if (!destDir.mkdirs()) {
				LOGGER.warn(
						"Unable to create dest dir {} for {}, may cause later problems ",
						destDir, target);
			}
		}
		File destFile = new File(destDir, target);
		if (!destFile.exists()) {
			InputStream in = Utils.class.getClassLoader().getResourceAsStream(
					target);
			OutputStream out = new FileOutputStream(destFile);
			IOUtils.copy(in, out);
			out.close();
			in.close();
		}
	}

	@Deactivate
	public void deactivate(ComponentContext componentContext) {
		disable();
	}

	public void disable() {
		if (!enabled) {
			return;
		}
		nakamuraCore.close();
		coreContainer.shutdown();
		enabled = false;
		if (listener != null) {
			listener.disabled();
		}

	}

	public SolrServer getServer() {
		return server;
	}

	public SolrServer getUpdateServer() {
		return server;
	}

	public String getSolrHome() {
		return solrHome;
	}

	public String getName() {
		return EMBEDDED;
	}

}
