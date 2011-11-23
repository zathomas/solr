/**
 * Licensed to the Sakai Foundation (SF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The SF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.sakaiproject.nakamura.solr;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Dictionary;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;
import org.sakaiproject.nakamura.api.solr.SolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

@Component(immediate = true, metatype = true)
@Service(value = SolrClient.class)
public class RemoteSolrClient implements SolrClient {

	@Property(value = SolrClient.REMOTE)
	public static final String CLIENT_NAME = SolrClient.CLIENT_NAME;

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

	private static final Logger LOGGER = LoggerFactory.getLogger(RemoteSolrClient.class);

	private ThreadLocal<SolrServer> updateServer = new ThreadLocal<SolrServer>();
	private String solrHome;

	private Dictionary<String, Object> properties;

	private boolean enabled;

	private SolrClientListener listener;

	private SolrServer queryServer;

	@SuppressWarnings("unchecked")
	@Activate
	public void activate(ComponentContext componentContext) throws IOException {
		BundleContext bundleContext = componentContext.getBundleContext();
		properties = componentContext
		.getProperties();
		solrHome = Utils.getSolrHome(bundleContext);
	}
		
	public void enable(SolrClientListener listener) throws IOException,
		ParserConfigurationException, SAXException {
		if ( enabled ) {
			return;
		}
		queryServer = createQueryServer();
		enabled = true;
		this.listener = listener;
	}
	
	
	private SolrServer createQueryServer() throws MalformedURLException {
		
		String url = Utils.toString(properties.get(PROP_SOLR_URL),
				"http://localhost:8983/solr");
		StreamingUpdateSolrServer server = new StreamingUpdateSolrServer(url, Utils.toInt(
				properties.get(PROP_QUEUE_SIZE), 100), Utils.toInt(
				properties.get(PROP_THREAD_COUNT), 10));
		server.setSoTimeout(Utils.toInt(properties.get(PROP_SO_TIMEOUT),
				1000)); // socket
						// read
						// timeout
		server.setConnectionTimeout(Utils.toInt(
				properties.get(PROP_CONNECTION_TIMEOUT), 100));
		server.setDefaultMaxConnectionsPerHost(Utils.toInt(
				properties.get(PROP_MAX_CONNECTONS_PER_HOST), 100));
		server.setMaxTotalConnections(Utils.toInt(
				properties.get(PROP_MAX_TOTAL_CONNECTONS), 100));
		server.setFollowRedirects(Utils.toBoolean(
				properties.get(PROP_FOLLOW), false)); // defaults
														// to
														// false
		// allowCompression defaults to false.
		// Server side must support gzip or deflate for this to have any effect.
		server.setAllowCompression(Utils.toBoolean(
				properties.get(PROP_ALLOW_COMPRESSION), true));
		server.setMaxRetries(Utils.toInt(
				properties.get(PROP_MAX_RETRIES), 1)); // defaults
														// to 0.
														// > 1
														// not
														// recommended.
		server.setParser(new BinaryResponseParser()); // binary parser is used
														// by default
		return server;
	}

	
	private SolrServer createUpdateServer() throws MalformedURLException {
		
		String url = Utils.toString(properties.get(PROP_SOLR_URL),
				"http://localhost:8983/solr");
		CommonsHttpSolrServer server = new CommonsHttpSolrServer(url);
		server.setSoTimeout(Utils.toInt(properties.get(PROP_SO_TIMEOUT),
				1000)); // socket
						// read
						// timeout
		server.setConnectionTimeout(Utils.toInt(
				properties.get(PROP_CONNECTION_TIMEOUT), 100));
		server.setDefaultMaxConnectionsPerHost(Utils.toInt(
				properties.get(PROP_MAX_CONNECTONS_PER_HOST), 100));
		server.setMaxTotalConnections(Utils.toInt(
				properties.get(PROP_MAX_TOTAL_CONNECTONS), 100));
		server.setFollowRedirects(Utils.toBoolean(
				properties.get(PROP_FOLLOW), false)); // defaults
														// to
														// false
		// allowCompression defaults to false.
		// Server side must support gzip or deflate for this to have any effect.
		server.setAllowCompression(Utils.toBoolean(
				properties.get(PROP_ALLOW_COMPRESSION), true));
		server.setMaxRetries(Utils.toInt(
				properties.get(PROP_MAX_RETRIES), 1)); // defaults
														// to 0.
														// > 1
														// not
														// recommended.
		server.setParser(new BinaryResponseParser()); // binary parser is used
														// by default
		return server;
	}
	
	

	@Deactivate
	public void deactivate(ComponentContext componentContext) {
		disable();
	}
	
	public void disable() {
		if ( !enabled ) {
			return;
		}
		enabled = false;
		if ( listener != null ) {
			listener.disabled();
		}
	}


	public SolrServer getServer() {
		return queryServer;
	}

	public String getSolrHome() {
		return solrHome;
	}

	public SolrServer getUpdateServer() {
		SolrServer solrServer = updateServer.get();
		if ( solrServer == null ) {
			try {
				solrServer = createUpdateServer();
			} catch (MalformedURLException e) {
				LOGGER.error(e.getMessage(),e);
				return null;
			}
			updateServer.set(solrServer);
		}
		return solrServer;
	}

	public String getName() {
		return REMOTE;
	}


}
