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

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;
import org.sakaiproject.nakamura.api.solr.SolrServerService;

import java.io.IOException;
import java.util.Dictionary;

@Component(enabled = false, immediate=true, metatype=true)
@Service(value=SolrServerService.class)
public class RemoteSolrClient implements SolrServerService {

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
  
  
  
  private StreamingUpdateSolrServer server;
  private String solrHome;

  @Activate
  public void activate(ComponentContext componentContext) throws IOException {
    BundleContext bundleContext = componentContext.getBundleContext();
    @SuppressWarnings("unchecked")
    Dictionary<String, Object> properties = componentContext.getProperties();
    String url = PropertiesUtil.toString(properties.get(PROP_SOLR_URL),
        "http://localhost:8983/solr");
    server = new StreamingUpdateSolrServer(url, PropertiesUtil.toInteger(
        properties.get(PROP_QUEUE_SIZE), 100), PropertiesUtil.toInteger(
        properties.get(PROP_THREAD_COUNT), 10));
    server.setSoTimeout(PropertiesUtil.toInteger(properties.get(PROP_SO_TIMEOUT), 1000)); // socket
                                                                                  // read
                                                                                  // timeout
    server.setConnectionTimeout(PropertiesUtil.toInteger(properties.get(PROP_CONNECTION_TIMEOUT),
        100));
    server.setDefaultMaxConnectionsPerHost(PropertiesUtil.toInteger(
        properties.get(PROP_MAX_CONNECTONS_PER_HOST), 100));
    server.setMaxTotalConnections(PropertiesUtil.toInteger(
        properties.get(PROP_MAX_TOTAL_CONNECTONS), 100));
    server.setFollowRedirects(PropertiesUtil.toBoolean(properties.get(PROP_FOLLOW), false)); // defaults
                                                                                     // to
                                                                                     // false
    // allowCompression defaults to false.
    // Server side must support gzip or deflate for this to have any effect.
    server.setAllowCompression(PropertiesUtil.toBoolean(properties.get(PROP_ALLOW_COMPRESSION),
        true));
    server.setMaxRetries(PropertiesUtil.toInteger(properties.get(PROP_MAX_RETRIES), 1)); // defaults
                                                                                 // to 0.
                                                                                 // > 1
                                                                                 // not
                                                                                 // recommended.
    server.setParser(new BinaryResponseParser()); // binary parser is used by default
    solrHome = Utils.getSolrHome(bundleContext);
  }

  @Deactivate
  public void deactivate(ComponentContext componentContext) {
  }

  public SolrServer getServer() {
    return server;
  }

  public String getSolrHome() {
    return solrHome;
  }

  public SolrServer getUpdateServer() {
    return server;
  }
}
