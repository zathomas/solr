package org.sakaiproject.nakamura.solr;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
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
public class MultiMasterRemoteSolrClient implements SolrClient {

	private static final String DEFAULT_SOLR_URLS = "http://localhost:8983/solr";
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MultiMasterRemoteSolrClient.class);
	@Property(value = SolrClient.MULTIREMOTE)
	public static final String CLIENT_NAME = SolrClient.CLIENT_NAME;

	@Property(value = DEFAULT_SOLR_URLS )
	private static final String PROP_SOLR_URL = "remoteurl";

	@Property(value = { DEFAULT_SOLR_URLS })
	private static final String PROP_QUERY_SOLR_URLS = "query-urls";
	
	@Property(intValue = 1000 )
	private static final String PROP_QUERY_SO_TIMEOUT = "query-so-timeout";
	@Property(intValue = 100 )
	private static final String PROP_QUERY_CONNECTION_TIMEOUT = "query-connection-timeout";
	@Property(intValue = 100 )
	private static final String PROP_QUERY_ALIVE_CHECK_INTERVAL = "query-check-alive-interval";
	@Property(intValue = 100 )
	private static final String PROP_QUERY_CONNECTION_MANAGER_TIMEOUT = "query-connection-manager-timeout";

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

	private static final String LOGGER_KEY = "org.sakaiproject.nakamura.logger";
	private static final String LOGGER_VAL = "org.apache.solr";

	/**
	 * According to the doc, this is thread safe and must be shared between all
	 * threads.
	 */
	private SolrServer server;
	/**
	 * This should be the Solr server that accepts updates. This might be local
	 * or it might be remote, depending on election.
	 */
	private ThreadLocal<SolrServer> updateServer = new ThreadLocal<SolrServer>();

	@Reference
	protected ConfigurationAdmin configurationAdmin;
	private Map<String, Object> multiMasterProperties;
	private boolean enabled;
	private SolrClientListener listener;
	private String solrHome;

	@Activate
	public void activate(ComponentContext componentContext) throws IOException,
			ParserConfigurationException, SAXException {
		BundleContext bundleContext = componentContext.getBundleContext();
		multiMasterProperties = toMap(componentContext
				.getProperties());
		solrHome = Utils.getSolrHome(bundleContext);
	}

	public void enable(SolrClientListener listener) throws IOException,
			ParserConfigurationException, SAXException {
		if (enabled) {
			return;
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

		server = createQueryServer(multiMasterProperties);
		this.enabled = true;
		this.listener = listener;
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
				DEFAULT_SOLR_URLS);

		CommonsHttpSolrServer remoteServer = new CommonsHttpSolrServer(url);
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

	private SolrServer createQueryServer(Map<String, Object> properties)
			throws MalformedURLException {
		String[] serverUrls = Utils.toStringArray(
				properties.get(PROP_QUERY_SOLR_URLS),
				new String[] { DEFAULT_SOLR_URLS });

		LBHttpSolrServer lbServer = new LBHttpSolrServer(serverUrls);
		lbServer.setSoTimeout(Utils.toInt(
				properties.get(PROP_QUERY_SO_TIMEOUT), 1000)); // socket
		// read
		// timeout
		lbServer.setConnectionTimeout(Utils.toInt(
				properties.get(PROP_QUERY_CONNECTION_TIMEOUT), 100));
		lbServer.setAliveCheckInterval(Utils.toInt(
				properties.get(PROP_QUERY_ALIVE_CHECK_INTERVAL), 100));
		lbServer.setConnectionManagerTimeout(Utils.toInt(
				properties.get(PROP_QUERY_CONNECTION_MANAGER_TIMEOUT), 100));
		return lbServer;
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


	@Deactivate
	public void deactivate(ComponentContext componentContext) {
		disable();
	}

	public void disable() {
		if (!enabled) {
			return;
		}
		enabled = false;
		if (listener != null) {
			listener.disabled();
		}
	}

	public SolrServer getServer() {
		return server;
	}

	public SolrServer getUpdateServer() {
		SolrServer solrServer = updateServer.get();
		if ( solrServer == null ) {
			try {
				solrServer = createUpdateServer(multiMasterProperties);
			} catch (MalformedURLException e) {
				LOGGER.error(e.getMessage(),e);
				return null;
			}
			updateServer.set(solrServer);
		}
		return solrServer;
	}

	public String getSolrHome() {
		return solrHome;
	}

	public String getName() {
		return MULTIREMOTE;
	}

}
