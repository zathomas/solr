package org.sakaiproject.nakamura.api.solr;

import org.apache.solr.client.solrj.SolrServer;

public interface SolrServerService {

  SolrServer getServer();

  String getSolrHome();

}
