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
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.JSON_RESULTS;
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.PARAMS_ITEMS_PER_PAGE;
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.REG_BATCH_PROCESSOR_NAMES;
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.REG_PROCESSOR_NAMES;
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.REG_PROVIDER_NAMES;
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.SAKAI_BATCHRESULTPROCESSOR;
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.SAKAI_LIMIT_RESULTS;
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.SAKAI_PROPERTY_PROVIDER;
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.SAKAI_QUERY_TEMPLATE;
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.SAKAI_RESULTPROCESSOR;
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.SEARCH_BATCH_RESULT_PROCESSOR;
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.SEARCH_PATH_PREFIX;
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.SEARCH_PROPERTY_PROVIDER;
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.SEARCH_RESULT_PROCESSOR;
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.TIDY;
import static org.sakaiproject.nakamura.api.solr.search.SolrSearchConstants.TOTAL;

import org.apache.commons.lang.StringUtils;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.References;
import org.apache.felix.scr.annotations.sling.SlingServlet;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.request.RequestParameter;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.servlets.SlingSafeMethodsServlet;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.io.JSONWriter;
import org.apache.sling.jcr.base.util.AccessControlUtil;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
import org.osgi.service.component.ComponentContext;
import org.sakaiproject.nakamura.api.doc.ServiceDocumentation;
import org.sakaiproject.nakamura.api.doc.ServiceMethod;
import org.sakaiproject.nakamura.api.doc.ServiceParameter;
import org.sakaiproject.nakamura.api.doc.ServiceResponse;
import org.sakaiproject.nakamura.api.personal.PersonalUtils;
import org.sakaiproject.nakamura.api.profile.ProfileService;
import org.sakaiproject.nakamura.api.solr.search.MissingParameterException;
import org.sakaiproject.nakamura.api.solr.search.Result;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchBatchResultProcessor;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchException;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchPropertyProvider;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchResultProcessor;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchResultSet;
import org.sakaiproject.nakamura.api.solr.search.SolrSearchUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.ValueFormatException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

/**
 * The <code>SearchServlet</code> uses nodes from the
 * 
 */
@ServiceDocumentation(name = "Search Servlet", shortDescription = "The Search servlet provides search results.", description = {
    "The Search Servlet responds with search results in json form in response to GETs on search urls. Those URLs are resolved "
        + "as resources of type sakai/search. The node at the resource containing properties that represent a search template that is "
        + "used to perform the search operation. This allows the UI developer to create nodes in the JCR and configure those nodes to "
        + "act as an end point for a search based view into the JCR. If the propertyprovider or the batchresultprocessor are not specified, "
        + "default implementations will be used.",
    "The format of the template is ",
    "<pre>"
        + " nt:unstructured \n"
        + "        -sakai:query-template - a message query template for the query, wiht placeholders \n"
        + "                                for parameters of the form {request-parameter-name}\n"
        + "        -sakai:query-language - either XPATH or SQL depending on the dialect used for the query\n"
        + "        -sakai:propertyprovider - the name of a Property Provider used to populate the properties \n"
        + "                                  to be used in the query \n"
        + "        -sakai:batchresultprocessor - the name of a SearchResultProcessor to be used processing \n"
        + "                                      the result set.\n" + "</pre>",
    "For example:",
    "<pre>" + "/var/search/content\n" + "{  \n"
        + "   \"sakai:query-language\": \"xpath\", \n"
        + "   \"sakai:query-template\": \"//*[jcr:contains(.,\\\"{q}\\\")]\", \n"
        + "   \"sling:resourceType\": \"sakai/search\", \n"
        + "   \"sakai:resultprocessor\": \"Node\" \n" + "} \n" + "</pre>" }, methods = { @ServiceMethod(name = "GET", description = {
    "Processes the query request against the selected resource, using the properties on the resource as a "
        + "template for processing the request and a specification for the pre and post processing steps on the search."
        + " results.",
    "For example",
    "<pre>" + "curl http://localhost:8080/var/search/content.json?q=a\n" + "{\n"
        + "  \"query\": \"//*[jcr:contains(.,\\\"a\\\")]\",\n" + "  \"items\": 25,\n"
        + "  \"total\": 56,\n" + "  \"results\": [\n" + "      {\n"
        + "          \"jcr:data\": \"org.apache.jackrabbit.value.BinaryValue@0\",\n"
        + "          \"jcr:primaryType\": \"nt:resource\",\n"
        + "          \"jcr:mimeType\": \"text/plain\",\n"
        + "          \"jcr:uuid\": \"0b6bd369-f0dd-4eb3-87cb-7fa8e079cccf\",\n"
        + "          \"jcr:lastModified\": \"2009-11-24T11:55:51\"\n" + "      },\n"
        + "      {\n" + "          \"sakai:is-site-template\": \"true\",\n"
        + "          \"sakai:authorizables\": [\n"
        + "              \"g-temp-collaborators\",\n"
        + "              \"g-temp-viewers\"\n" + "          ],\n"
        + "          \"description\": \"This is a template!\",\n"
        + "          \"id\": \"template\",\n"
        + "          \"sling:resourceType\": \"sakai/site\",\n"
        + "          \"sakai:site-template\": \"/dev/_skins/original/original.html\",\n"
        + "          \"jcr:mixinTypes\": [\n"
        + "              \"rep:AccessControllable\"\n" + "          ],\n"
        + "          \"jcr:primaryType\": \"nt:unstructured\",\n"
        + "          \"status\": \"online\",\n" + "          \"name\": \"template\"\n"
        + "      },\n" + "      ...\n" + "      {\n"
        + "          \"jcr:data\": \"org.apache.jackrabbit.value.BinaryValue@0\",\n"
        + "          \"jcr:primaryType\": \"nt:resource\",\n"
        + "          \"jcr:mimeType\": \"text/html\",\n"
        + "          \"jcr:uuid\": \"a9b46582-b30c-4489-b9e3-8fdc20cb5429\",\n"
        + "          \"jcr:lastModified\": \"2009-11-24T11:55:51\"\n" + "      }\n"
        + "  ]\n" + "}\n" + "</pre>" }, parameters = {
    @ServiceParameter(name = "items", description = { "The number of items per page in the result set." }),
    @ServiceParameter(name = "page", description = { "The page number to start listing the results on." }),
    @ServiceParameter(name = "*", description = { "Any other parameters may be used by the template." }) }, response = {
    @ServiceResponse(code = 200, description = "A search response simular to the above will be emitted "),
    @ServiceResponse(code = 403, description = "The search template is not located under /var "),
    @ServiceResponse(code = 400, description = "There are too many results that need to be paged. "),
    @ServiceResponse(code = 500, description = "Any error with the html containing the error")

}) })
@SlingServlet(extensions = { "json" }, methods = { "GET" }, resourceTypes = { "sakai/solr-search" })
@Properties(value = {
    @Property(name = "service.description", value = { "Perfoms searchs based on the associated node." }),
    @Property(name = "service.vendor", value = { "The Sakai Foundation" }),
    @Property(name = "maximumResults", longValue = 2500L) })
@References(value = {
    @Reference(name = "SearchResultProcessor", referenceInterface = SolrSearchResultProcessor.class, bind = "bindSearchResultProcessor", unbind = "unbindSearchResultProcessor", cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE, policy = ReferencePolicy.DYNAMIC),
    @Reference(name = "SearchBatchResultProcessor", referenceInterface = SolrSearchBatchResultProcessor.class, bind = "bindSearchBatchResultProcessor", unbind = "unbindSearchBatchResultProcessor", cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE, policy = ReferencePolicy.DYNAMIC),
    @Reference(name = "SearchPropertyProvider", referenceInterface = SolrSearchPropertyProvider.class, bind = "bindSearchPropertyProvider", unbind = "unbindSearchPropertyProvider", cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE, policy = ReferencePolicy.DYNAMIC) })
public class SolrSearchServlet extends SlingSafeMethodsServlet {

  /**
   *
   */
  private static final long serialVersionUID = 4130126304725079596L;
  private static final Logger LOGGER = LoggerFactory.getLogger(SolrSearchServlet.class);


  private Map<String, SolrSearchBatchResultProcessor> batchProcessors = new ConcurrentHashMap<String, SolrSearchBatchResultProcessor>();
  private Map<Long, SolrSearchBatchResultProcessor> batchProcessorsById = new ConcurrentHashMap<Long, SolrSearchBatchResultProcessor>();

  private Map<String, SolrSearchResultProcessor> processors = new ConcurrentHashMap<String, SolrSearchResultProcessor>();
  private Map<Long, SolrSearchResultProcessor> processorsById = new ConcurrentHashMap<Long, SolrSearchResultProcessor>();

  private Map<String, SolrSearchPropertyProvider> propertyProvider = new ConcurrentHashMap<String, SolrSearchPropertyProvider>();
  private Map<Long, SolrSearchPropertyProvider> propertyProviderById = new ConcurrentHashMap<Long, SolrSearchPropertyProvider>();

  private transient ComponentContext osgiComponentContext;
  private List<ServiceReference> delayedReferences = new ArrayList<ServiceReference>();
  private List<ServiceReference> delayedPropertyReferences = new ArrayList<ServiceReference>();
  private List<ServiceReference> delayedBatchReferences = new ArrayList<ServiceReference>();

  protected long maximumResults = 100;

  // Default processors
  /**
   * Reference uses property set on NodeSearchResultProcessor. Other processors can become
   * the default by setting {@link SearchResultProcessor.DEFAULT_PROCESOR_PROP} to true.
   */
  private static final String DEFAULT_BATCH_SEARCH_PROC_TARGET = "(&("
      + SolrSearchBatchResultProcessor.DEFAULT_BATCH_PROCESSOR_PROP + "=true))";
  @Reference(target = DEFAULT_BATCH_SEARCH_PROC_TARGET)
  protected SolrSearchBatchResultProcessor defaultSearchBatchProcessor;

  /**
   * Reference uses property set on NodeSearchResultProcessor. Other processors can become
   * the default by setting {@link SearchResultProcessor.DEFAULT_PROCESOR_PROP} to true.
   */
  private static final String DEFAULT_SEARCH_PROC_TARGET = "(&("
      + SolrSearchResultProcessor.DEFAULT_PROCESSOR_PROP + "=true))";
  @Reference(target = DEFAULT_SEARCH_PROC_TARGET)
  protected SolrSearchResultProcessor defaultSearchProcessor;


  @Reference
  protected transient ProfileService profileService;


  private Pattern homePathPattern = Pattern.compile("^(.*)(~([\\w-]*?))/");

  @Override
  public void init() throws ServletException {
    super.init();
  }

  @Override
  protected void doGet(SlingHttpServletRequest request, SlingHttpServletResponse response)
      throws ServletException, IOException {
    try {
      Resource resource = request.getResource();
      if (!resource.getPath().startsWith(SEARCH_PATH_PREFIX)) {
        response.sendError(HttpServletResponse.SC_FORBIDDEN,
            "Search templates can only be executed if they are located under "
                + SEARCH_PATH_PREFIX);
        return;
      }

      Node node = resource.adaptTo(Node.class);
      if (node != null && node.hasProperty(SAKAI_QUERY_TEMPLATE)) {
        String queryTemplate = node.getProperty(SAKAI_QUERY_TEMPLATE).getString();
        String propertyProviderName = null;
        if (node.hasProperty(SAKAI_PROPERTY_PROVIDER)) {
          propertyProviderName = node.getProperty(SAKAI_PROPERTY_PROVIDER).getString();
        }
        boolean limitResults = true;
        if (node.hasProperty(SAKAI_LIMIT_RESULTS)) {
          limitResults = node.getProperty(SAKAI_LIMIT_RESULTS).getBoolean();
        }

        long nitems = SolrSearchUtil.longRequestParameter(request, PARAMS_ITEMS_PER_PAGE,
            DEFAULT_PAGED_ITEMS);
        

        // KERN-1147 Response better when all parameters haven't been provided for a query
        String queryString = null;
        try {
          queryString = processQueryTemplate(request, node, queryTemplate,
              propertyProviderName);
        } catch (MissingParameterException e) {
          response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
          return;
        }

        queryString = expandHomeDirectoryInQuery(node, queryString);



        boolean useBatch = false;
        // Get the
        SolrSearchBatchResultProcessor searchBatchProcessor = defaultSearchBatchProcessor;
        if (node.hasProperty(SAKAI_BATCHRESULTPROCESSOR)) {
          searchBatchProcessor = batchProcessors.get(node.getProperty(
              SAKAI_BATCHRESULTPROCESSOR).getString());
          useBatch = true;
          if (searchBatchProcessor == null) {
            searchBatchProcessor = defaultSearchBatchProcessor;
          }
        }

        SolrSearchResultProcessor searchProcessor = defaultSearchProcessor;
        if (node.hasProperty(SAKAI_RESULTPROCESSOR)) {
          searchProcessor = processors.get(node.getProperty(SAKAI_RESULTPROCESSOR)
              .getString());
          if (searchProcessor == null) {
            searchProcessor = defaultSearchProcessor;
          }
        }

        SolrSearchResultSet rs = null;
        try {
          // Prepare the result set.
          // This allows a processor to do other queries and manipulate the results.
          if (useBatch) {
            rs = searchBatchProcessor.getSearchResultSet(request, queryString);
            if (!(rs instanceof SolrSearchResultSetImpl)) {
              SolrSearchException ex = new SolrSearchException(
                  500,
                  "Invalid Implementation  "
                      + searchBatchProcessor
                      + " is not creating a SearchResultSet using the SearchServiceFactory ");
              LOGGER.error(ex.getMessage(), ex);
              throw ex;
            }
          } else {
            rs = searchProcessor.getSearchResultSet(request, queryString);
            if (!(rs instanceof SolrSearchResultSetImpl)) {
              SolrSearchException ex = new SolrSearchException(
                  500,
                  "Invalid Implementation  "
                      + searchProcessor
                      + " is not creating a SearchResultSet using the SearchServiceFactory ");
              LOGGER.error(ex.getMessage(), ex);
              throw ex;
            }
          }
        } catch (SolrSearchException e) {
          response.sendError(e.getCode(), e.getMessage());
          return;
        }

        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");

        JSONWriter write = new JSONWriter(response.getWriter());
        write.setTidy(isTidy(request));
        
        write.object();
        write.key(PARAMS_ITEMS_PER_PAGE);
        write.value(nitems);
        write.key(JSON_RESULTS);

        write.array();

        Iterator<Result> iterator = rs.getResultSetIterator();
        if (useBatch) {
          LOGGER.info("Using batch processor for results");
          searchBatchProcessor.writeResults(request, write, iterator);
        } else {
          LOGGER.info("Using regular processor for results");
          // We don't skip any rows ourselves here.
          // We expect a rowIterator coming from a resultset to be at the right place.
          for (long i = 0; i < nitems && iterator.hasNext(); i++) {
            // Get the next row.
            Result result = iterator.next();

            // Write the result for this row.
            searchProcessor.writeResult(request, write, result);
          }
        }
        write.endArray();

        // write the total out after processing the list to give the underlying iterator
        // a chance to walk the results then report how many there were.
        write.key(TOTAL);
        write.value(rs.getSize());

        write.endObject();
      }
    } catch (RepositoryException e) {
      e.printStackTrace();
      response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
      LOGGER.info("Caught RepositoryException {}", e.getMessage());
    } catch (JSONException e) {
      response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
      LOGGER.info("Caught JSONException {}", e.getMessage());
    }
  }

  private String expandHomeDirectoryInQuery(Node node, String queryString)
      throws RepositoryException {
    Matcher homePathMatcher = homePathPattern.matcher(queryString);
    if (homePathMatcher.find()) {
      String username = homePathMatcher.group(3);
      String homePrefix = homePathMatcher.group(1);
      UserManager um = AccessControlUtil.getUserManager(node.getSession());
      Authorizable au = um.getAuthorizable(username);
      String homePath = homePrefix + profileService.getHomePath(au).substring(1) + "/";
      queryString = homePathMatcher.replaceAll(homePath);
    }
    return queryString;
  }

  /**
   * Processes a template of the form select * from y where x = {q} so that strings
   * enclosed in { and } are replaced by the same property in the request.
   * 
   * @param request
   *          the request.
   * @param queryTemplate
   *          the query template.
   * @param propertyProviderName
   * @return A processed query template
   * @throws ValueFormatException
   * @throws RepositoryException
   */
  protected String processQueryTemplate(SlingHttpServletRequest request,
      Node queryTemplateNode, String queryTemplate, String propertyProviderName)
      throws MissingParameterException {
    Map<String, String> propertiesMap = loadUserProperties(request, propertyProviderName);
    Map<String, String> filteredRequestParametersMap = new HashMap<String, String>();



    StringBuilder sb = new StringBuilder();
    boolean escape = false;
    int vstart = -1;
    char[] ca = queryTemplate.toCharArray();
    String defaultValue = null;
    for (int i = 0; i < ca.length; i++) {
      char c = ca[i];
      if (escape) {
        sb.append(c);
        escape = false;
      } else if (vstart >= 0) {
        if (c == '}') {
          String v = new String(ca, vstart + 1, i - vstart - 1);
          defaultValue = null;
          // Take care of default values
          if (v.contains("|")) {
            String[] val = v.split("\\|");
            v = val[0];
            defaultValue = val[1];
          }
          boolean optional = false;
          if (v.endsWith("?")) {
            optional = true;
            v = v.substring(0, v.length() - 1);
          }
          if (v.startsWith("_")) {
            String value = propertiesMap.get(v);
            if (!StringUtils.isEmpty(value)) {
              sb.append(value);
            } else if (StringUtils.isEmpty(value) && !StringUtils.isEmpty(defaultValue)) {
              sb.append(defaultValue);
            } else if (!optional) {
              throw new MissingParameterException("Unable to substitute {" + v
                  + "} in query template");
            }
          } else {
            String rpVal = filteredRequestParametersMap.get(v);
            if (rpVal == null) {
              RequestParameter rp = request.getRequestParameter(v);
              if (rp != null) {
                rpVal = rp.getString();
              }
            }

            if ("sortOn".equals(v) && !StringUtils.isEmpty(rpVal)) {
              if (defaultValue.startsWith("@")) {
                LOGGER
                    .warn("Invalid Search template, you cant use sortOn parameters that "
                        + "could produce sorts on child nodes as this is likely to stop the server dead, ignoring sort order and using default ");
                rpVal = null;
              }
              if (rpVal.indexOf('/') >= 0 || rpVal.indexOf('@') >= 0) {
                LOGGER.warn("Attempt to sort on child node, {}, ignoring ", rpVal);
                rpVal = null;
              }
            }

            if (!StringUtils.isEmpty(rpVal)) {
              sb.append(escapeString(rpVal));
            } else if (StringUtils.isEmpty(rpVal) && !StringUtils.isEmpty(defaultValue)) {
              sb.append(escapeString(defaultValue));
            } else if (!optional) {
              throw new MissingParameterException("Unable to substitute {" + v
                  + "} in query template");
            }
          }
          vstart = -1;
        }
      } else {
        switch (c) {
        case '{':
          vstart = i;
          break;
        case '\\':
          escape = true;
          break;
        default:
          sb.append(c);
        }
      }
    }

    return sb.toString();
  }

  private String escapeString(String rpVal) {
    return rpVal;
  }


  /**
   * @param request
   * @param propertyProviderName
   * @return
   * @throws RepositoryException
   */
  private Map<String, String> loadUserProperties(SlingHttpServletRequest request,
      String propertyProviderName) {
    Map<String, String> propertiesMap = new HashMap<String, String>();
    String userId = request.getRemoteUser();
    Session session = request.getResourceResolver().adaptTo(Session.class);
    try {
      UserManager um = AccessControlUtil.getUserManager(session);
      Authorizable au = um.getAuthorizable(userId);
      String userPrivatePath = PersonalUtils.getPrivatePath(au);
      propertiesMap.put("_userPrivatePath", userPrivatePath);
    } catch (RepositoryException e) {
      LOGGER.error("Unable to get the authorizable for this user.", e);
    }
    propertiesMap.put("_userId", userId);
    if (propertyProviderName != null) {
      LOGGER.debug("Trying Provider Name {} ", propertyProviderName);
      SolrSearchPropertyProvider provider = propertyProvider.get(propertyProviderName);
      if (provider != null) {
        LOGGER.debug("Trying Provider {} ", provider);
        provider.loadUserProperties(request, propertiesMap);
      } else {
        LOGGER.warn("No properties provider found for {} ", propertyProviderName);
      }
    } else {
      LOGGER.debug("No Provider ");
    }
    return propertiesMap;
  }

  protected void bindSearchResultProcessor(ServiceReference serviceReference) {
    synchronized (delayedReferences) {
      if (osgiComponentContext == null) {
        delayedReferences.add(serviceReference);
      } else {
        addProcessor(serviceReference);
      }
    }

  }

  protected void unbindSearchResultProcessor(ServiceReference serviceReference) {
    synchronized (delayedReferences) {
      if (osgiComponentContext == null) {
        delayedReferences.remove(serviceReference);
      } else {
        removeProcessor(serviceReference);
      }
    }

  }

  protected void bindSearchBatchResultProcessor(ServiceReference serviceReference) {
    synchronized (delayedBatchReferences) {
      if (osgiComponentContext == null) {
        delayedBatchReferences.add(serviceReference);
      } else {
        addBatchProcessor(serviceReference);
      }
    }

  }

  protected void unbindSearchBatchResultProcessor(ServiceReference serviceReference) {
    synchronized (delayedBatchReferences) {
      if (osgiComponentContext == null) {
        delayedBatchReferences.remove(serviceReference);
      } else {
        removeBatchProcessor(serviceReference);
      }
    }

  }

  protected void bindSearchPropertyProvider(ServiceReference serviceReference) {
    synchronized (delayedReferences) {
      if (osgiComponentContext == null) {
        delayedPropertyReferences.add(serviceReference);
      } else {
        addProvider(serviceReference);
      }
    }

  }

  protected void unbindSearchPropertyProvider(ServiceReference serviceReference) {
    synchronized (delayedReferences) {
      if (osgiComponentContext == null) {
        delayedPropertyReferences.remove(serviceReference);
      } else {
        removeProvider(serviceReference);
      }
    }

  }

  /**
   * @param serviceReference
   */
  private void removeProcessor(ServiceReference serviceReference) {
    Long serviceId = (Long) serviceReference.getProperty(Constants.SERVICE_ID);
    SolrSearchResultProcessor processor = processorsById.remove(serviceId);
    if (processor != null) {
      List<String> toRemove = new ArrayList<String>();
      for (Entry<String, SolrSearchResultProcessor> e : processors.entrySet()) {
        if (processor.equals(e.getValue())) {
          toRemove.add(e.getKey());
        }
      }
      for (String r : toRemove) {
        processors.remove(r);
      }

      // bit of a kludge until I can figure out why felix doesn't wire up the default
      // processor even though it finds a matching service.
      boolean defaultProcessor = getSetting(
          serviceReference.getProperty(SolrSearchResultProcessor.DEFAULT_PROCESSOR_PROP),
          false);
      if (defaultProcessor) {
        defaultSearchProcessor = null;
      }
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T getSetting(Object o, T defaultValue) {
    if (o == null) {
      return defaultValue;
    }
    return (T) o;
  }

  private String[] getSetting(Object o, String[] defaultValue) {
    if (o == null) {
      return defaultValue;
    }
    if (o.getClass().isArray()) {
      return (String[]) o;      
    }
    return new String[]{(String) o};
  }

  /**
   * @param serviceReference
   */
  private void addProcessor(ServiceReference serviceReference) {
    SolrSearchResultProcessor processor = (SolrSearchResultProcessor) osgiComponentContext
        .locateService(SEARCH_RESULT_PROCESSOR, serviceReference);
    Long serviceId = (Long) serviceReference.getProperty(Constants.SERVICE_ID);

    processorsById.put(serviceId, processor);
    String[] processorNames = getSetting(
        serviceReference.getProperty(REG_PROCESSOR_NAMES), new String[0]);

    for (String processorName : processorNames) {
      processors.put(processorName, processor);
    }

    // bit of a kludge until I can figure out why felix doesn't wire up the default
    // processor even though it finds a matching service.
    boolean defaultProcessor = getSetting(
        serviceReference.getProperty(SolrSearchResultProcessor.DEFAULT_PROCESSOR_PROP),
        false);
    if (defaultProcessor) {
      defaultSearchProcessor = processor;
    }
  }

  /**
   * @param serviceReference
   */
  private void removeBatchProcessor(ServiceReference serviceReference) {
    Long serviceId = (Long) serviceReference.getProperty(Constants.SERVICE_ID);
    SolrSearchResultProcessor processor = processorsById.remove(serviceId);
    if (processor != null) {
      List<String> toRemove = new ArrayList<String>();
      for (Entry<String, SolrSearchResultProcessor> e : processors.entrySet()) {
        if (processor.equals(e.getValue())) {
          toRemove.add(e.getKey());
        }
      }
      for (String r : toRemove) {
        processors.remove(r);
      }

      // bit of a kludge until I can figure out why felix doesn't wire up the default
      // processor even though it finds a matching service.
      boolean defaultBatchProcessor = getSetting(serviceReference
          .getProperty(SolrSearchBatchResultProcessor.DEFAULT_BATCH_PROCESSOR_PROP),
          false);
      if (defaultBatchProcessor) {
        defaultSearchBatchProcessor = null;
      }
    }
  }

  /**
   * @param serviceReference
   */
  private void addBatchProcessor(ServiceReference serviceReference) {
    SolrSearchBatchResultProcessor processor = (SolrSearchBatchResultProcessor) osgiComponentContext
        .locateService(SEARCH_BATCH_RESULT_PROCESSOR, serviceReference);
    Long serviceId = (Long) serviceReference.getProperty(Constants.SERVICE_ID);

    batchProcessorsById.put(serviceId, processor);
    String[] processorNames = getSetting(serviceReference
        .getProperty(REG_BATCH_PROCESSOR_NAMES), new String[0]);

    if (processorNames != null) {
      for (String processorName : processorNames) {
        batchProcessors.put(processorName, processor);
      }
    }

    // bit of a kludge until I can figure out why felix doesn't wire up the default
    // processor even though it finds a matching service.
    boolean defaultBatchProcessor = getSetting(serviceReference
        .getProperty(SolrSearchBatchResultProcessor.DEFAULT_BATCH_PROCESSOR_PROP), false);
    if (defaultBatchProcessor) {
      defaultSearchBatchProcessor = processor;
    }
  }

  /**
   * @param serviceReference
   */
  private void removeProvider(ServiceReference serviceReference) {
    Long serviceId = (Long) serviceReference.getProperty(Constants.SERVICE_ID);
    SolrSearchPropertyProvider provider = propertyProviderById.remove(serviceId);
    if (provider != null) {
      List<String> toRemove = new ArrayList<String>();
      for (Entry<String, SolrSearchPropertyProvider> e : propertyProvider.entrySet()) {
        if (provider.equals(e.getValue())) {
          toRemove.add(e.getKey());
        }
      }
      for (String r : toRemove) {
        propertyProvider.remove(r);
      }
    }
  }

  /**
   * @param serviceReference
   */
  private void addProvider(ServiceReference serviceReference) {
    SolrSearchPropertyProvider provider = (SolrSearchPropertyProvider) osgiComponentContext
        .locateService(SEARCH_PROPERTY_PROVIDER, serviceReference);
    Long serviceId = (Long) serviceReference.getProperty(Constants.SERVICE_ID);

    propertyProviderById.put(serviceId, provider);
    String[] processorNames = getSetting(serviceReference
        .getProperty(REG_PROVIDER_NAMES), new String[0]);

    for (String processorName : processorNames) {
      propertyProvider.put(processorName, provider);
    }
  }

  protected void activate(ComponentContext componentContext) {

    synchronized (delayedReferences) {
      osgiComponentContext = componentContext;
      for (ServiceReference ref : delayedReferences) {
        addProcessor(ref);
      }
      delayedReferences.clear();
    }
    synchronized (delayedBatchReferences) {
      osgiComponentContext = componentContext;
      for (ServiceReference ref : delayedBatchReferences) {
        addBatchProcessor(ref);
      }
      delayedBatchReferences.clear();
    }
    synchronized (delayedPropertyReferences) {
      osgiComponentContext = componentContext;
      for (ServiceReference ref : delayedPropertyReferences) {
        addProvider(ref);
      }
      delayedPropertyReferences.clear();
    }

    maximumResults = (Long) componentContext.getProperties().get("maximumResults");
  }

  /**
   * True if our request wants the "tidy" pretty-printed format Copied from
   * org.apache.sling.servlets.get.impl.helpers.JsonRendererServlet
   */
  protected boolean isTidy(SlingHttpServletRequest req) {
    for (String selector : req.getRequestPathInfo().getSelectors()) {
      if (TIDY.equals(selector)) {
        return true;
      }
    }
    return false;
  }

}
