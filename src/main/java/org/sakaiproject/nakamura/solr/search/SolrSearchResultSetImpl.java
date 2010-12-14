package org.sakaiproject.nakamura.solr.search;

import com.google.common.collect.UnmodifiableIterator;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.sakaiproject.nakamura.api.solr.search.Result;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchResultSet;

import java.util.Iterator;

public class SolrSearchResultSetImpl implements SolrSearchResultSet {

  private QueryResponse response;
  private SolrDocumentList responseList;
  public SolrSearchResultSetImpl(QueryResponse response) {
    this.response = response;
  }

  public Iterator<Result> getResultSetIterator() {
    loadResponse();
    final Iterator<SolrDocument> solrIterator = responseList.iterator();
    return new UnmodifiableIterator<Result>() {

      public boolean hasNext() {
        return solrIterator.hasNext();
      }

      public Result next() {
        return new ResultImpl(solrIterator.next());
      }
      
    };
  }


  public long getSize() {
    loadResponse();
    return responseList.getNumFound();
  }
  
  private void loadResponse() {
    if ( responseList == null ) {
      responseList = response.getResults();
    }
  }


}
