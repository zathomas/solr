/*
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
package org.sakaiproject.nakamura.solr.search;

import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.DEFAULT_PAGED_ITEMS;
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.PARAMS_ITEMS_PER_PAGE;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.commons.json.io.JSONWriter;
import org.sakaiproject.nakamura.api.solr.search.ResourceJsonWriter;
import org.sakaiproject.nakamura.api.solr.search.Result;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchBatchResultProcessor;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchException;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchResultSet;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchServiceFactory;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchUtil;

import java.util.Iterator;

@Component(immediate = true, metatype=true)
@Properties(value = {
    @Property(name = "service.vendor", value = "The Sakai Foundation"),
    @Property(name = SolrSearchConstants.REG_BATCH_PROCESSOR_NAMES, value = "Resource"),
    @Property(name = SolrSearchBatchResultProcessor.DEFAULT_BATCH_PROCESSOR_PROP, boolValue = true)
})
@Service(value = SolrSearchBatchResultProcessor.class)
public class DefaultSearchBatchResultProcessor implements
    SolrSearchBatchResultProcessor {

  @Reference
  protected SolrSearchServiceFactory searchServiceFactory;

  /**
   * The non component constructor
   * @param searchServiceFactory
   */
  DefaultSearchBatchResultProcessor(SolrSearchServiceFactory searchServiceFactory) {
    if ( searchServiceFactory == null ) {
      throw new IllegalArgumentException("Search Service Factory must be set when not using as a component");
    }
    this.searchServiceFactory = searchServiceFactory;
  }


  /**
   * Component Constructor.
   */
  public DefaultSearchBatchResultProcessor() {
  }



  public void writeResults(SlingHttpServletRequest request, JSONWriter write,
      Iterator<Result> iterator) {
    ResourceResolver resolver = request.getResourceResolver();
    
    
    long nitems = SolrSearchUtil.longRequestParameter(request,
        PARAMS_ITEMS_PER_PAGE, DEFAULT_PAGED_ITEMS);
    
    

    int maxDepth = SolrSearchUtil.getTraversalDepth(request);
    for (long i = 0; i < nitems && iterator.hasNext(); i++) {
      Result row = iterator.next();
      Resource resource = resolver.getResource(row.getPath());
      if ( resource != null ) {
        ResourceJsonWriter.writeResourceTreeToWriter(write, resource, maxDepth);                    
      }
    }
  }


  public SolrSearchResultSet getSearchResultSet(SlingHttpServletRequest request, String query) throws SolrSearchException {
    return searchServiceFactory.getSearchResultSet(request, query);  }

}
