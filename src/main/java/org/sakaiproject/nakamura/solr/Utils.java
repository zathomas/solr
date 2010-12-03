package org.sakaiproject.nakamura.solr;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Utils {

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  @SuppressWarnings("unchecked")
  public static <T> T getSetting(Object setting, T defaultSetting) {
    if (setting == null) {
      return defaultSetting;
    }
    return (T) setting;
  }

  public static String getParentPath(String path) {
    if ("/".equals(path)) {
      return "/";
    }
    int i = path.lastIndexOf('/');
    if (i == path.length() - 1) {
      i = path.substring(0, i).lastIndexOf('/');
    }
    String res = path;
    if (i > 0) {
      res = path.substring(0, i);
    } else if (i == 0) {
      return "/";
    }
    return res;
  }

  public static String getSolrHome(BundleContext bundleContext) throws IOException {
    String slingHomePath = bundleContext.getProperty("sling.home");
    File solrHome = new File(slingHomePath, "solr");
    if (!solrHome.isDirectory()) {
      if (!solrHome.mkdirs()) {
        LOGGER
            .info(
                "verifyConfiguration: Cannot create Solr home {}, failed creating default configuration ",
                solrHome.getAbsolutePath());
        return null;
      }
    }
    File solrXml = new File(solrHome, "solr.xml");
    if ( !solrXml.exists() ) {
      InputStream in = Utils.class.getClassLoader().getResourceAsStream("solr.xml");
      OutputStream out = new FileOutputStream(solrXml);
      IOUtils.copy(in,out);
      out.close();
      in.close();
    }
    return solrHome.getAbsolutePath();
  }

 
}
