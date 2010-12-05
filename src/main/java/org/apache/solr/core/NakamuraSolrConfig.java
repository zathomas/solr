package  org.apache.solr.core;

import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.ParserConfigurationException;

public class NakamuraSolrConfig extends SolrConfig {

  public NakamuraSolrConfig(SolrResourceLoader loader, String name, InputStream is)
      throws ParserConfigurationException, IOException, SAXException {
    super(loader, name, is);
  }

}
