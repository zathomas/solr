package org.sakaiproject.nakamura.solr;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Map;

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
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.apache.solr.core.CoreContainer;
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

@Component(immediate = true, metatype = true)
@Service(value = SolrClient.class)
public class MultiMasterSolrClient implements SolrClient {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(MultiMasterSolrClient.class);
	@Property(value = SolrClient.MULTI)
	public static final String CLIENT_NAME = SolrClient.CLIENT_NAME;

	@Property(value = "embedded")
	private static final String PROP_CLUSTER_CONFIG_MODE = "cluster.config.mode";

	@Property(value = "solrconfig.xml")
	private static final String PROP_SOLR_CONFIG = "solrconfig";
	@Property(value = "solrconfig.xml")
	private static final String PROP_SOLR_SCHEMA = "solrschema";

	@Property(value = "http://localhost:8983/solr")
	private static final String PROP_SOLR_URL = "remoteurl";

	@Property(intValue = 1)
	private static final String PROP_MAX_RETRIES = "max.retries";

	@Property(boolValue = true)
	private static final String PROP_ALLOW_COMPRESSION = "allow.compression";

	@Property(boolValue = false)
	private static final String PROP_FOLLOW = "follow.redirects";

	@Property(intValue = 100)
	private static final String PROP_MAX_TOTAL_CONNECTONS = "max.total.connections";

	@Property(intValue = 100)
	private static final String PROP_MAX_CONNECTONS_PER_HOST = "max.connections.per.host";

	@Property(intValue = 100)
	private static final String PROP_CONNECTION_TIMEOUT = "connection.timeout";

	@Property(intValue = 1000)
	private static final String PROP_SO_TIMEOUT = "socket.timeout";

	@Property(intValue = 100)
	private static final String PROP_QUEUE_SIZE = "indexer.queue.size";

	@Property(intValue = 10)
	private static final String PROP_THREAD_COUNT = "indexer.thread.count";

	private static final String LOGGER_KEY = "org.sakaiproject.nakamura.logger";
	private static final String LOGGER_VAL = "org.apache.solr";

	/**
	 * According to the doc, this is thread safe and must be shared between all
	 * threads.
	 */
	private EmbeddedSolrServer server;
	/**
	 * This should be the Solr server that accepts updates. This might be local
	 * or it might be remote, depending on election.
	 */
	private SolrServer updateServer;
	private String solrHome;
	private CoreContainer coreContainer;
	private SolrCore nakamuraCore;

	@Reference
	protected ConfigurationAdmin configurationAdmin;
	private Map<String, Object> multiMasterProperties;
	private boolean enabled;
	private SolrClientListener listener;

	@Activate
	public void activate(ComponentContext componentContext) throws IOException,
			ParserConfigurationException, SAXException {
		BundleContext bundleContext = componentContext.getBundleContext();
		solrHome = Utils.getSolrHome(bundleContext);
		multiMasterProperties = getMultiMasterProperties(toMap(componentContext
				.getProperties()));
	}

	public void enable(SolrClientListener listener) throws IOException,
			ParserConfigurationException, SAXException {
		if (enabled) {
			return;
		}

		String configLocation = "solrconfig.xml";
		String schemaLocation = "schema.xml";
		if (multiMasterProperties != null) {
			configLocation = (String) multiMasterProperties
					.get(PROP_SOLR_CONFIG);
			schemaLocation = (String) multiMasterProperties
					.get(PROP_SOLR_SCHEMA);
		}

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
			configSource = getStream(configLocation);
			schemaSource = getStream(schemaLocation);
			SolrConfig config = new NakamuraSolrConfig(loader, configLocation,
					configSource);
			IndexSchema schema = new IndexSchema(config, null, schemaSource);
			nakamuraCore = new SolrCore("nakamura", coreDir.getAbsolutePath(),
					config, schema, null);
			coreContainer.register("nakamura", nakamuraCore, false);
			server = new EmbeddedSolrServer(coreContainer, "nakamura");
			LoggerFactory.getLogger(this.getClass()).info("Contans cores {} ",
					coreContainer.getCoreNames());
		} finally {
			Thread.currentThread().setContextClassLoader(contextClassloader);
			safeClose(schemaSource);
			safeClose(configSource);
		}

		if (multiMasterProperties == null) {
			updateServer = server;
		} else {
			updateServer = createUpdateServer(multiMasterProperties);
		}
		this.enabled = true;
		this.listener = listener;
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

	/**
	 * @param map
	 * @return
	 */
	private Map<String, Object> getMultiMasterProperties(
			Map<String, Object> properties) {
		String clusterConfigMode = Utils.toString(
				properties.get(PROP_CLUSTER_CONFIG_MODE), "election");
		if ("embedded".equals(clusterConfigMode)) {
			return null;
		} else if ("election".equals(clusterConfigMode)) {
			// perform an election and get the properties from the master node.
			// TODO
		}
		// is the config mode is not election or embedded, then its configured
		// on a per node
		// basis and already in the properties.
		// properties contain the master configuration.
		return properties;
	}

	private Map<String, Object> toMap(
			@SuppressWarnings("rawtypes") Dictionary properties) {
		Builder<String, Object> b = ImmutableMap.builder();
		for (Enumeration<?> e = properties.keys(); e.hasMoreElements();) {
			String k = (String) e.nextElement();
			b.put(k, properties.get(k));
		}
		return b.build();
	}

	private SolrServer createUpdateServer(Map<String, Object> properties)
			throws MalformedURLException {
		String url = Utils.toString(properties.get(PROP_SOLR_URL),
				"http://localhost:8983/solr");

		StreamingUpdateSolrServer remoteServer = new StreamingUpdateSolrServer(
				url, Utils.toInt(properties.get(PROP_QUEUE_SIZE), 100),
				Utils.toInt(properties.get(PROP_THREAD_COUNT), 10));
		remoteServer.setSoTimeout(Utils.toInt(
				properties.get(PROP_SO_TIMEOUT), 1000)); // socket
		// read
		// timeout
		remoteServer.setConnectionTimeout(Utils.toInt(
				properties.get(PROP_CONNECTION_TIMEOUT), 100));
		remoteServer.setDefaultMaxConnectionsPerHost(Utils.toInt(
				properties.get(PROP_MAX_CONNECTONS_PER_HOST), 100));
		remoteServer.setMaxTotalConnections(Utils.toInt(
				properties.get(PROP_MAX_TOTAL_CONNECTONS), 100));
		remoteServer.setFollowRedirects(Utils.toBoolean(
				properties.get(PROP_FOLLOW), false)); // defaults
		// to
		// false
		// allowCompression defaults to false.
		// Server side must support gzip or deflate for this to have any effect.
		remoteServer.setAllowCompression(Utils.toBoolean(
				properties.get(PROP_ALLOW_COMPRESSION), true));
		remoteServer.setMaxRetries(Utils.toInt(
				properties.get(PROP_MAX_RETRIES), 1)); // defaults
		// to 0.
		// > 1
		// not
		// recommended.
		remoteServer.setParser(new BinaryResponseParser()); // binary parser is
															// used by
															// default

		return remoteServer;
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

	private ClosableInputSource getStream(String name) throws IOException {
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
		return updateServer;
	}

	public String getSolrHome() {
		return solrHome;
	}

	public String getName() {
		return MULTI;
	}

}
