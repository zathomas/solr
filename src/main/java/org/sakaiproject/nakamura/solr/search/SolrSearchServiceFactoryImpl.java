package org.sakaiproject.nakamura.solr.search;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.sakaiproject.nakamura.api.solr.SolrServerService;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchException;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchResultSet;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchServiceFactory;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(immediate=true, metatype=true)
@Service
public class SolrSearchServiceFactoryImpl implements SolrSearchServiceFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(SolrSearchServiceFactoryImpl.class);
  @Reference
  private SolrServerService solrSearchSearvice;

  public SolrSearchResultSet getSearchResultSet(SlingHttpServletRequest request,
      String query) throws SolrSearchException {
    SolrQuery solrQuery = new SolrQuery(query);
    long[] ranges = SolrSearchUtil.getOffsetAndSize(request);
    solrQuery.setStart((int) ranges[0]);
    solrQuery.setRows((int) ranges[1]);
    
    SolrServer solrServer = solrSearchSearvice.getServer();
    
    try {
      return new SolrSearchResultSetImpl(solrServer.query(solrQuery));
    } catch (SolrServerException e) {
      LOGGER.warn(e.getMessage(),e);
      throw new SolrSearchException(500, e.getMessage());
    }
  }

}
