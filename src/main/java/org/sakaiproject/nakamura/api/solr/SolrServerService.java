package org.sakaiproject.nakamura.api.solr;

import org.apache.solr.client.solrj.SolrServer;

/**
 * A service to manage the SolrServer implementation.
 */
public interface SolrServerService {

  /**
   * @return the Current Solr Server, which might be embedded or might be remote depending
   *         on the implementation of the service.
   */
  SolrServer getServer();

  /**
   * @return the location of the Solr Home.
   */
  String getSolrHome();

}
