package org.sakaiproject.nakamura.api.solr;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.client.solrj.SolrServer;
import org.sakaiproject.nakamura.solr.SolrClientListener;
import org.xml.sax.SAXException;

public interface SolrClient {

	public static final String EMBEDDED = "embedded";
	public static final String REMOTE = "remote";
	public static final String MULTI = "multi";
	public static final String CLIENT_NAME = "client.name";

	String getName();

	// this is not the same interface as SolrServerService to enable us to have
	// all solr clients active at the same time.
	/**
	 * @return the Current Solr Server, which might be embedded or might be
	 *         remote depending on the implementation of the service.
	 */
	SolrServer getServer();

	/**
	 * @return the Solr Server used to perform updates.
	 */
	SolrServer getUpdateServer();

	/**
	 * @return the location of the Solr Home.
	 */
	String getSolrHome();

	void enable(SolrClientListener listener) throws IOException,
			ParserConfigurationException, SAXException;

	void disable();

}
