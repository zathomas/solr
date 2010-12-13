package org.sakaiproject.nakamura.api.solr.search;

import org.apache.sling.api.SlingHttpServletRequest;

import java.util.Map;

public interface SolrSearchPropertyProvider {

  void loadUserProperties(SlingHttpServletRequest request,
      Map<String, String> propertiesMap);

}
