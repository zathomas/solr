package org.sakaiproject.nakamura.solr;

import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrResourceLoader;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NakamuraSolrResourceLoader extends SolrResourceLoader {

  static final String[] packages = {"","analysis.","schema.","handler.","search.","update.","core.","response.","request.","update.processor.","util.", "spelling.", "handler.component.", "handler.dataimport." };
  private static Map<String, String> classNameCache = new ConcurrentHashMap<String, String>();
  static final String project = "solr";
  static final String base = "org.apache" + "." + project;

  

  public NakamuraSolrResourceLoader( String instanceDir, ClassLoader parent ) {
    super(instanceDir,parent);
  }

  /**
   * This method loads a class either with it's FQN or a short-name (solr.class-simplename or class-simplename).
   * It tries to load the class with the name that is given first and if it fails, it tries all the known
   * solr packages. This method caches the FQN of a short-name in a static map in-order to make subsequent lookups
   * for the same class faster. The caching is done only if the class is loaded by the webapp classloader and it
   * is loaded using a shortname.
   *
   * @param cname The name or the short name of the class.
   * @param subpackages the packages to be tried if the cnams starts with solr.
   * @return the loaded class. An exception is thrown if it fails
   */
  public Class findClass(String cname, String... subpackages) {
    if (subpackages == null || subpackages.length == 0 || subpackages == packages) {
      subpackages = packages;
      String  c = classNameCache.get(cname);
      if(c != null) {
        try {
          return Class.forName(c, true, classLoader);
        } catch (ClassNotFoundException e) {
          //this is unlikely
          log.error("Unable to load cached class-name :  "+ c +" for shortname : "+cname + e);
        }

      }
    }
    Class clazz = null;
    // first try cname == full name
    try {
      return Class.forName(cname, true, classLoader);
    } catch (ClassNotFoundException e) {
      String newName=cname;
      if (newName.startsWith(project)) {
        newName = cname.substring(project.length()+1);
      }
      for (String subpackage : subpackages) {
        try {
          String name = base + '.' + subpackage + newName;
          log.trace("Trying class name " + name);
          return clazz = Class.forName(name,true,classLoader);
        } catch (ClassNotFoundException e1) {
          // ignore... assume first exception is best.
        }
      }
  
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "Error loading class '" + cname + "'", e, false);
    }finally{
      //cache the shortname vs FQN if it is loaded by the webapp classloader  and it is loaded
      // using a shortname
      if ( clazz != null &&
              clazz.getClassLoader() == SolrResourceLoader.class.getClassLoader() &&
              !cname.equals(clazz.getName()) &&
              (subpackages.length == 0 || subpackages == packages)) {
        //store in the cache
        classNameCache.put(cname, clazz.getName());
      }
    }
  }
  
  
  @Override
  public InputStream openResource(String resource) {
    InputStream in = this.getClass().getClassLoader().getResourceAsStream(resource);
    if ( in == null ) {
      in = super.openResource(resource);
    }
    return in;
  }

}
