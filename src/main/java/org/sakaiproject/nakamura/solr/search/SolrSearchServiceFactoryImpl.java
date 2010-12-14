package org.sakaiproject.nakamura.solr.search;

import com.google.common.collect.Lists;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.jcr.base.util.AccessControlUtil;
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

import java.util.Iterator;
import java.util.List;

import javax.jcr.RepositoryException;
import javax.jcr.Session;

@Component(immediate = true, metatype = true)
@Service
public class SolrSearchServiceFactoryImpl implements SolrSearchServiceFactory {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SolrSearchServiceFactoryImpl.class);
  @Reference
  private SolrServerService solrSearchSearvice;

  public SolrSearchResultSet getSearchResultSet(SlingHttpServletRequest request,
      String query) throws SolrSearchException {
    try {
      SolrQuery solrQuery = new SolrQuery(query);
      long[] ranges = SolrSearchUtil.getOffsetAndSize(request);
      solrQuery.setStart((int) ranges[0]);
      solrQuery.setRows((int) ranges[1]);

      // apply readers restrictions.
      Session session = request.getResourceResolver().adaptTo(Session.class);
      UserManager userManager = AccessControlUtil.getUserManager(session);
      User user = (User) userManager.getAuthorizable(session.getUserID());
      List<String> readers = Lists.newArrayList();
      for ( Iterator<Group> gi = user.memberOf(); gi.hasNext(); ) {
        readers.add(gi.next().getID());
      }
      solrQuery.add("readers", readers.toArray(new String[readers.size()]));
      
      
      SolrServer solrServer = solrSearchSearvice.getServer();

      return new SolrSearchResultSetImpl(solrServer.query(solrQuery));
    } catch (SolrServerException e) {
      LOGGER.warn(e.getMessage(), e);
      throw new SolrSearchException(500, e.getMessage());
    } catch (RepositoryException e) {
      LOGGER.warn(e.getMessage(), e);
      throw new SolrSearchException(500, e.getMessage());
    }
  }

}
