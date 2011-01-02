package org.sakaiproject.nakamura.solr;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang.StringUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.felix.scr.annotations.Services;
import org.apache.sling.jcr.api.SlingRepository;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventConstants;
import org.osgi.service.event.EventHandler;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.Repository;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.solr.Indexer;
import org.sakaiproject.nakamura.api.solr.IndexingHandler;
import org.sakaiproject.nakamura.api.solr.RepositorySession;
import org.sakaiproject.nakamura.api.solr.SolrServerService;
import org.sakaiproject.nakamura.api.solr.TopicIndexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.jcr.RepositoryException;
import javax.jcr.Session;

@Component(immediate = true, metatype = true)
@Services(value = { @Service(value = EventHandler.class), @Service(value = TopicIndexer.class) })
public class ContentEventListener implements EventHandler, TopicIndexer, Runnable {

  @Property(value = "org/apache/sling/api/resource/Resource/*", propertyPrivate = true)
  static final String TOPICS = EventConstants.EVENT_TOPIC;

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ContentEventListener.class);

  private static final String END = "--end--";

  @Reference
  protected SolrServerService solrServerService;

  @Reference
  protected SlingRepository repository;

  @Reference
  protected Repository sparseRepository;

  private Map<String, IndexingHandler> handlers = Maps.newConcurrentHashMap();

  private Session session;

  private File currentFile;

  private FileWriter eventWriter;

  private File logDirectory;

  private BufferedReader eventReader;

  private File currentInFile;

  private Object waitingForFileLock = new Object();

  private boolean running = true;

  private File positionFile;

  private int lineNo;

  private Thread queueDispatcher;

  private long nread;

  private long nwrite;

  private org.sakaiproject.nakamura.api.lite.Session sparseSession;

  private RepositorySession repositorySession;

  @Activate
  protected void activate(Map<String, Object> properties) throws RepositoryException,
      IOException, ClientPoolException, StorageClientException, AccessDeniedException {
    session = repository.loginAdministrative(null);
    sparseSession = sparseRepository.loginAdministrative();
    repositorySession = new RepositorySession() {
      
      @SuppressWarnings("unchecked")
      public <T> T adaptTo(Class<T> c) {
        if ( c.equals(Session.class)) {
          return (T) session;
        } 
        if ( c.equals(org.sakaiproject.nakamura.api.lite.Session.class)) {
          return (T) sparseSession;
        }
        return null;
      }
    };
    logDirectory = new File(solrServerService.getSolrHome(), "indexq");
    positionFile = new File(solrServerService.getSolrHome(), "indexqpos");
    if (!logDirectory.isDirectory()) {
      if (!logDirectory.mkdirs()) {
        LOGGER.warn("Failed to create {} ", logDirectory.getAbsolutePath());
        throw new IOException("Faild to create Indexing Log directory "
            + logDirectory.getAbsolutePath());
      }
    }
    loadPosition();
    queueDispatcher = new Thread(this);
    queueDispatcher.setName("IndexerQueueDispatch");
    queueDispatcher.start();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          closeWriter();
        } catch (Exception e) {

        }
      }
    });

  }

  @Deactivate
  protected void deactivate(Map<String, Object> properties) throws IOException {
    if (session != null) {
      session.logout();
    }
    if (sparseSession != null) {
      try {
        sparseSession.logout();
      } catch (ClientPoolException e) {
        LOGGER.warn(e.getMessage(), e);
      }
    }
    closeWriter();
    running = false;
    notifyReader();
  }

  public void closeWriter() throws IOException {
    if (eventWriter != null) {
      LOGGER.debug("Writer closing {} ", currentFile.getName());
      nwrite++;
      eventWriter.append(END).append("\n");
      eventWriter.flush();
      eventWriter.close();
      eventWriter = null;
      notifyReader();
    }
  }

  public Thread getQueueDispatcher() {
    return queueDispatcher;
  }

  public void handleEvent(Event event) {
    String topic = event.getTopic();
    LOGGER.debug("Got Event {} {} ", event, handlers);
    IndexingHandler contentIndexHandler = handlers.get(topic);
    if (contentIndexHandler != null) {
      try {
        saveEvent(event);
      } catch (IOException e) {
        LOGGER.info(e.getMessage(), e);
      }
    }
  }

  private void saveEvent(Event event) throws IOException {
    LOGGER.debug("Save Event {} ", event);
    if (currentFile != null && currentFile.length() > 1024 * 1024) {
      LOGGER.debug("Closed {} ", currentFile.getName());
      nwrite++;
      eventWriter.append(END);
      eventWriter.close();
      eventWriter = null;
      currentFile = null;
    }
    if (currentFile == null) {
      currentFile = new File(logDirectory, String.valueOf(System.currentTimeMillis()));
    }
    if (eventWriter == null) {
      eventWriter = new FileWriter(currentFile);
      LOGGER.debug("Opened {} ", currentFile.getName());
    }
    String[] properties = event.getPropertyNames();
    String[] op = new String[properties.length * 2 + 1];
    op[0] = URLEncoder.encode(event.getTopic(), "UTF8");
    int i = 1;
    for (String p : properties) {
      op[i] = URLEncoder.encode(p, "UTF8");
      ;
      i++;
      op[i] = URLEncoder.encode(String.valueOf(event.getProperty(p)), "UTF8");
      i++;
    }
    eventWriter.append(StringUtils.join(op, ',')).append('\n');
    nwrite++;
    eventWriter.flush();
    notifyReader();
  }

  private void notifyReader() {
    synchronized (waitingForFileLock) {
      waitingForFileLock.notify();
    }
  }

  public void run() {
    while (running) {
      try {
        Event event = readEvent();
        String topic = event.getTopic();
        IndexingHandler contentIndexHandler = handlers.get(topic);
        LOGGER.debug("Got Handler {} for event {}", contentIndexHandler, event);
        if (contentIndexHandler != null) {
          SolrServer service = solrServerService.getServer();
          try {
            boolean needsCommit = false;
            for (String deleteQuery : contentIndexHandler
                .getDeleteQueries(repositorySession, event)) {
              if (service != null) {
                LOGGER.debug("Added delete Query {} ", deleteQuery);
                try {
                  service.deleteByQuery(deleteQuery);
                  needsCommit = true;
                } catch (SolrServerException e) {
                  LOGGER.info(" Failed to delete {}  cause :{}", deleteQuery,
                      e.getMessage());
                }
              }
            }
            Collection<SolrInputDocument> docs = contentIndexHandler.getDocuments(repositorySession, event);
            if (service != null) {
              if (docs != null && docs.size() > 0) {
                LOGGER.debug("Adding Docs {} ", docs);
                service.add(docs);
                needsCommit = true;
              }
            }
            if (needsCommit) {
              service.commit();
            }

            savePosition();
          } catch (SolrServerException e) {
            try {
              service.rollback();
            } catch (Exception e1) {
              LOGGER.warn(e.getMessage(), e1);
            }
          } catch (IOException e) {
            LOGGER.warn(e.getMessage(), e);
          } catch (SolrException e) {
            LOGGER.warn(e.getMessage(), e);
          }
        } else {
          savePosition();
        }
      } catch (IOException e) {
        if (running) {
          LOGGER.warn(e.getMessage(), e);
        } else {
          LOGGER.debug("Closing Down Indexer Event Queue");
        }
      }
    }

  }

  private Event readEvent() throws IOException {
    String line = nextEvent();
    String[] parts = StringUtils.split(line, ',');
    Dictionary<String, Object> dict = new Hashtable<String, Object>();
    for (int i = 1; i < parts.length; i += 2) {
      dict.put(URLDecoder.decode(parts[i], "UTF8"),
          URLDecoder.decode(parts[i + 1], "UTF8"));
    }
    return new Event(URLDecoder.decode(parts[0], "UTF8"), dict);
  }

  private String nextEvent() throws IOException {
    checkReaderOpen();
    String line = null;
    while (line == null || END.equals(line)) {
      if (END.equals(line)) {
        nextReader();
      }
      line = eventReader.readLine();
      if (line != null) {
        nread++;
        lineNo++;
      } else {
        waitForWriter();
      }
    }
    return line;
  }

  private void savePosition() throws IOException {
    FileWriter position = new FileWriter(positionFile);
    position.write(URLEncoder.encode(currentInFile.getAbsolutePath(), "UTF8"));
    position.write(",");
    position.write(String.valueOf(lineNo));
    position.write("\n");
    position.close();
  }

  private void loadPosition() throws IOException {
    if (positionFile.exists()) {
      BufferedReader position = new BufferedReader(new FileReader(positionFile));
      String[] filePos = StringUtils.split(position.readLine(), ',');
      if (filePos != null && filePos.length == 2) {
        currentInFile = new File(URLDecoder.decode(filePos[0], "UTF8"));
        if (currentInFile.exists()) {
          lineNo = Integer.parseInt(filePos[1]);
          eventReader = new BufferedReader(new FileReader(currentInFile));
          for (int i = 0; i < lineNo; i++) {
            eventReader.readLine();
          }
          return;
        }
      }
    }
    currentInFile = null;
    currentFile = null;
    lineNo = 0;
  }

  private void checkReaderOpen() throws IOException {
    while (currentInFile == null) {
      List<File> files = Lists.newArrayList(logDirectory.listFiles());
      Collections.sort(files, new Comparator<File>() {

        public int compare(File o1, File o2) {
          return (int) (o1.lastModified() - o2.lastModified());
        }
      });
      if (files.size() > 0) {
        LOGGER.debug("Reader currently {} MB behind ", files.size());
        currentInFile = files.get(0);
        if (eventReader != null) {
          eventReader.close();
          eventReader = null;
        }
      } else {
        LOGGER.debug("No More files ");
        waitForWriter();
      }
    }
    if (eventReader == null) {
      LOGGER.debug("Opening New Reader {} ", currentInFile);
      eventReader = new BufferedReader(new FileReader(currentInFile));
      lineNo = 0;
    }
  }

  private void nextReader() throws IOException {
    if (eventReader != null) {
      LOGGER.debug("Closing Reader File {} ", currentInFile);
      eventReader.close();
      eventReader = null;
    }
    if (currentInFile != null) {
      LOGGER.info("Deleting Reader File {} ", currentInFile);
      currentInFile.delete();
      positionFile.delete();
      currentInFile = null;
    }
    checkReaderOpen();
  }

  private void waitForWriter() throws IOException {
    if (running) {
      synchronized (waitingForFileLock) {
        try {
          LOGGER.debug("Waiting for more data read:{} written:{} ", nread, nwrite);
          if (nread > nwrite) {
            // reset counters if were catching up
            nread = nwrite;
          } else if (nread < nwrite) {

            // cehck that the file we are reading from is the newest file
            long currentFileModified = currentInFile.lastModified();
            File[] files = logDirectory.listFiles();
            for (File f : files) {
              if (f.lastModified() > currentFileModified) {
                nextReader();
                return;
              }
            }
            LOGGER
                .info(
                    "Event Reader is waiting while there are apparently records left to read, it may have missed some read:{} written:{}",
                    nread, nwrite);
          }
          waitingForFileLock.wait(5000);
        } catch (InterruptedException e) {
        }
      }
    }
    if (!running) {
      throw new IOException("Shutdown in pogress, aborting index queue reader");
    }
  }

  public void addHandler(String topic, IndexingHandler handler) {
    handlers.put(topic, handler);
  }

  public void removeHander(String topic, IndexingHandler handler) {
    if (handler.equals(handlers.get(topic))) {
      handlers.remove(topic);
    }
  }

}
