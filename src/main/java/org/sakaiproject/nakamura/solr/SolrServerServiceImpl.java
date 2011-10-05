package org.sakaiproject.nakamura.solr;

import java.io.IOException;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.ReferenceStrategy;
import org.apache.felix.scr.annotations.References;
import org.apache.felix.scr.annotations.Service;
import org.apache.solr.client.solrj.SolrServer;
import org.sakaiproject.nakamura.api.solr.SolrClient;
import org.sakaiproject.nakamura.api.solr.SolrServerService;
import org.xml.sax.SAXException;

import com.google.common.collect.Maps;

@Component(immediate = true, metatype = true)
@Service(value = SolrServerService.class)
@References(
		value={
				@Reference(target="(client.name=embedded)",name="embeddedClient", cardinality=ReferenceCardinality.MANDATORY_UNARY,policy=ReferencePolicy.STATIC,strategy=ReferenceStrategy.EVENT,bind="bind",unbind="unbind",referenceInterface=SolrClient.class),
				@Reference(target="(client.name=remote)",name="remoteClient", cardinality=ReferenceCardinality.MANDATORY_UNARY,policy=ReferencePolicy.STATIC,strategy=ReferenceStrategy.EVENT,bind="bind",unbind="unbind",referenceInterface=SolrClient.class),
				@Reference(target="(client.name=multi)",name="slitClient", cardinality=ReferenceCardinality.MANDATORY_UNARY,policy=ReferencePolicy.STATIC,strategy=ReferenceStrategy.EVENT,bind="bind",unbind="unbind",referenceInterface=SolrClient.class),
				@Reference(name="optionalClient", cardinality=ReferenceCardinality.OPTIONAL_MULTIPLE,policy=ReferencePolicy.DYNAMIC,strategy=ReferenceStrategy.EVENT,bind="bind",unbind="unbind",referenceInterface=SolrClient.class)
		})
public class SolrServerServiceImpl implements SolrServerService, SolrClientListener {

	
	@Property(value=SolrClient.EMBEDDED, description="embedded|remote|multi|other")
	private static final String SOLR_IMPL = "solr-impl";
	private SolrClient server;
	private Map<String, SolrClient> servers = Maps.newConcurrentMap();
	
	

	@Activate
	public void activate(Map<String, Object> properties) throws IOException, ParserConfigurationException, SAXException {
		modified(properties);
	}
	
	@Modified
	public void modified(Map<String, Object> properties) throws IOException, ParserConfigurationException, SAXException {
		String serverImplName = toString(properties.get(SOLR_IMPL), SolrClient.EMBEDDED);
		SolrClient newServer = servers.get(serverImplName);
		if ( newServer == null ) {
			throw new RuntimeException("Cant locate the Solr implementation called "+serverImplName);
		}
		newServer.enable(this);
		if ( server != null ) {
			server.disable();
		}
		server = newServer;
	}
	
	private String toString(Object object, String defaultValue) {
		if ( object == null ) {
			return defaultValue;
		}
		return String.valueOf(object);
	}

	public SolrServer getServer() {
		return server.getServer();
	}

	public SolrServer getUpdateServer() {
		return server.getUpdateServer();
	}

	public String getSolrHome() {
		return server.getSolrHome();
	}
	
	public void bind(SolrClient client) {
		servers.put(client.getName(), client);
		
	}
	
	public void unbind(SolrClient client) {
		servers.remove(client.getName());
	}

	public void disabled() {
		server = null;
	}

}
