package org.sakaiproject.nakamura.solr;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
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
import java.util.Map.Entry;
import java.util.Set;

import javax.jcr.RepositoryException;
import javax.jcr.Session;

@Component(immediate = true, metatype = true)
@Services(value = { @Service(value = EventHandler.class),
    @Service(value = TopicIndexer.class) })
public class ContentEventListener implements EventHandler, TopicIndexer, Runnable {

  @Property(intValue = 0)
  static final String BATCHED_INDEX_SIZE = "batched-index-size";
  
  @Property(longValue= 15000L)
  static final String BATCHED_INDEX_LIFETIME = "batched-index-flush-interval";

  @Property(value = { "org/sakaiproject/nakamura/lite/*",
      "org/apache/sling/api/resource/Resource/*" }, propertyPrivate = true)
  static final String TOPICS = EventConstants.EVENT_TOPIC;

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ContentEventListener.class);

  private static final String END = "--end--";

  private static final Integer DEFAULT_BATCHED_INDEX_SIZE = 100;


  private static final Long DEFAULT_BATCHED_INDEX_LIFETIME = 15000L;

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

  protected int batchedIndexSize;

  private Set<File> deleteQueue;

  private File savedCurrentInFile;

  private int savedLineNo;

  private long queueLifetime;


  @Activate
  protected void activate(Map<String, Object> properties) throws RepositoryException,
      IOException, ClientPoolException, StorageClientException, AccessDeniedException {
    session = repository.loginAdministrative(null);
    sparseSession = sparseRepository.loginAdministrative();
    batchedIndexSize = StorageClientUtils.getSetting(properties.get(BATCHED_INDEX_SIZE),
        DEFAULT_BATCHED_INDEX_SIZE);

    queueLifetime = StorageClientUtils.getSetting(properties.get(BATCHED_INDEX_LIFETIME),
        DEFAULT_BATCHED_INDEX_LIFETIME);

    repositorySession = new RepositorySession() {

      @SuppressWarnings("unchecked")
      public <T> T adaptTo(Class<T> c) {
        if (c.equals(Session.class)) {
          return (T) session;
        }
        if (c.equals(org.sakaiproject.nakamura.api.lite.Session.class)) {
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
    if (sparseSession != null) {
      try {
        sparseSession.logout();
        sparseSession = null;
      } catch (ClientPoolException e) {
        LOGGER.warn(e.getMessage(), e);
      }
    }
    if (session != null) {
      session.logout();
      session = null;
    }
    closeWriter();
    running = false;
    notifyReader();
  }

  public void closeWriter() throws IOException {
    if (eventWriter != null) {
      LOGGER.debug("Writer closing {} ", currentFile.getName());
      nwrite++;
      eventWriter.append(END);
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
        LOGGER.warn(e.getMessage(), e);
      }
    }
  }

  private void saveEvent(Event event) throws IOException {
    LOGGER.debug("Save Event {} ", event);
    if (currentFile != null && currentFile.length() > 1024 * 1024) {
      LOGGER.info("Closed Event Redo Log {} ", currentFile);
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
      LOGGER.info("Opened Event Redo Log {} ", currentFile);
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
    batchedEventRun();
  }

  private void batchedEventRun() {
    while (running) {
      try {
        begin();
        Event loadEvent = null;
        try { 
          loadEvent = readEvent(true);
        } catch ( Throwable t) {
          LOGGER.warn("Unreadble Event at {} {} ",currentInFile, lineNo);
        }
        Map<String, Event> events = Maps.newLinkedHashMap();
        long queueTTL = System.currentTimeMillis()+queueLifetime;
        while (loadEvent != null) {
          String topic = loadEvent.getTopic();
          String path = (String) loadEvent.getProperty("path");
          if (path != null) {
            IndexingHandler contentIndexHandler = handlers.get(topic);
            if (contentIndexHandler != null) {
              if (events.containsKey(path)) {
                // events is a linked hash map this will put it at the end.
                events.remove(path);
              }
              events.put(path, loadEvent);
            }
          }
          if (events.size() >= batchedIndexSize || queueTTL > System.currentTimeMillis() ) {
            break;
          }
          
          loadEvent = null;
          while ( loadEvent == null ) {
            try {
              loadEvent = readEvent(true);
            } catch ( Throwable t) {
              LOGGER.warn("Unreadble Event at {} {} ",currentInFile, lineNo);            
            }
          }
        }

        SolrServer service = solrServerService.getServer();
        try {
          boolean needsCommit = false;
          for (Entry<String, Event> ev : events.entrySet()) {
            Event event = ev.getValue();
            String topic = event.getTopic();
            IndexingHandler contentIndexHandler = handlers.get(topic);
            LOGGER.debug("Got Handler {} for event {} {}", new Object[] {
                contentIndexHandler, event, event.getProperty("path") });
            if (contentIndexHandler != null) {
              for (String deleteQuery : contentIndexHandler.getDeleteQueries(
                  repositorySession, event)) {
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
              Collection<SolrInputDocument> docs = contentIndexHandler.getDocuments(
                  repositorySession, event);
              if (service != null) {
                if (docs != null && docs.size() > 0) {
                  LOGGER.debug("Adding Docs {} ", docs);
                  service.add(docs);
                  needsCommit = true;
                }
              }
            }
          }
          if (needsCommit) {
            LOGGER.info("Processed {} events in a batch ", events.size());
            service.commit();
          }
          commit();
        } catch (SolrServerException e) {
          try {
            service.rollback();
          } catch (Exception e1) {
            LOGGER.warn(e.getMessage(), e1);
          }
          rollback();
        } catch (IOException e) {
          LOGGER.warn(e.getMessage(), e);
          rollback();
        } catch (SolrException e) {
          LOGGER.warn(e.getMessage(), e);
          rollback();
        }
      } catch (Throwable e) {
        if (running) {
          LOGGER.warn(e.getMessage(), e);
          try {
            rollback();
          } catch (IOException e1) {
            LOGGER.warn(e1.getMessage(), e1);
          }
        } else {
          LOGGER.debug("Closing Down Indexer Event Queue");
        }
      }
    }
  }

  private void perEventRun() {
    while (running) {
      try {
        Event event = readEvent(true);
        String topic = event.getTopic();
        IndexingHandler contentIndexHandler = handlers.get(topic);
        LOGGER.debug("Got Handler {} for event {} {}", new Object[] {
            contentIndexHandler, event, event.getProperty("path") });
        if (contentIndexHandler != null) {
          SolrServer service = solrServerService.getServer();
          try {
            boolean needsCommit = false;
            for (String deleteQuery : contentIndexHandler.getDeleteQueries(
                repositorySession, event)) {
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
            Collection<SolrInputDocument> docs = contentIndexHandler.getDocuments(
                repositorySession, event);
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

  private Event readEvent(boolean blocking) throws IOException {
    String line = nextEvent(blocking);
    if (line != null) {
      String[] parts = StringUtils.split(line, ',');
      if ( parts.length > 0 ) {
        Dictionary<String, Object> dict = new Hashtable<String, Object>();
        for (int i = 1; i < parts.length; i += 2) {
          dict.put(URLDecoder.decode(parts[i], "UTF8"),
              URLDecoder.decode(parts[i + 1], "UTF8"));
        }
        return new Event(URLDecoder.decode(parts[0], "UTF8"), dict);
      }
    }
    return null;
  }

  private String nextEvent(boolean blocking) throws IOException {
    String line = null;
    int possibleEnd = 0;
    if (checkReaderOpen(blocking)) {
      while (line == null || END.equals(line)) {
        if (END.equals(line)) {
          LOGGER.debug("At End of file {}", currentInFile);
          if (!nextReader(blocking)) {
            return null;
          }
        }
        line = eventReader.readLine();

        if (line != null) {
          possibleEnd = 0;
          nread++;
          lineNo++;
          if ((nread % 10000) == 0) {
            LOGGER.info("Event Redo Log has processed {} events", nread);
          }
        } else {
          // if we get null from a buffered reader that means end of file
          if (blocking) {
            if (possibleEnd == 1) {
              // even though the writer wrote something, we still couldnt read
              waitForWriter();
              possibleEnd = 2;
            } else if (possibleEnd == 2) {
              // try reopen the file, incase the Buffered reader is stale.
              LOGGER.info("Reopening file, currently {}", currentInFile);
              loadPosition(currentInFile, lineNo);
              possibleEnd = 3;
            } else if (possibleEnd == 3) {
              //we waited, we reloaded and its still not giving more, all I can assume is the file got closed without a new one, so go for the next reader
              LOGGER.info("Searching for next file, currently {}", currentInFile);
              nextReader(blocking);
              possibleEnd = 4;
            } else if ( possibleEnd == 4) {
              LOGGER.warn("Unable to process redo log, resetting reader");
              // total failure to read anything,
              return null;
            } else {
              waitForWriter();
              // the write wrote something
              possibleEnd = 1;
            }
          } else {
            return null;
          }
        }
      }
    }
    return line;
  }

  private void rollback() throws IOException {
    currentInFile = savedCurrentInFile;
    lineNo = savedLineNo;
    savePosition();
    if (eventReader != null) {
      eventReader.close();
      eventReader = null;
    }
    loadPosition(); // reopen the event reader to reset its position.
  }

  private void commit() throws IOException {
    if (deleteQueue != null) {
      for (File f : deleteQueue) {
        LOGGER.info("Deleting Reader File {} ", f);
        f.delete();
      }
      positionFile.delete();
      deleteQueue.clear();
      deleteQueue = null;
    }
    if (currentInFile != null) {
      savePosition();
    }
  }

  private void begin() {
    savedCurrentInFile = currentInFile;
    savedLineNo = lineNo;
    deleteQueue = Sets.newHashSet();
  }

  private void savePosition() throws IOException {
    if (currentInFile == null) {
      positionFile.delete();
    } else {
      FileWriter position = new FileWriter(positionFile);
      position.write(URLEncoder.encode(currentInFile.getAbsolutePath(), "UTF8"));
      position.write(",");
      position.write(String.valueOf(lineNo));
      position.write("\n");
      position.close();
    }
  }

  private void loadPosition() throws IOException {
    if (positionFile.exists()) {
      BufferedReader position = new BufferedReader(new FileReader(positionFile));
      String[] filePos = StringUtils.split(position.readLine(), ',');
      if (filePos != null && filePos.length == 2) {
        currentInFile = new File(URLDecoder.decode(filePos[0], "UTF8"));
        if (currentInFile.exists()) {
          lineNo = Integer.parseInt(filePos[1]);
          loadPosition(currentInFile, lineNo);
          return;
        }
      }
    }
    currentInFile = null;
    currentFile = null;
    lineNo = 0;
  }

  private void loadPosition(File file, int line) throws IOException {
    if (file != null) {
      if (eventReader != null) {
        eventReader.close();
      }
      eventReader = new BufferedReader(new FileReader(file));
      for (int i = 0; i < line; i++) {
        eventReader.readLine();
      }
    }
 }

  private boolean checkReaderOpen(boolean blocking) throws IOException {
    while (currentInFile == null) {
      List<File> files = Lists.newArrayList(logDirectory.listFiles());
      Collections.sort(files, new Comparator<File>() {

        public int compare(File o1, File o2) {
          return (int) (o1.lastModified() - o2.lastModified());
        }
      });
      if (deleteQueue != null) {
        files.removeAll(deleteQueue);
      }
      if (files.size() > 0) {
        if ( files.size() > 1 ) {
          LOGGER.info("Reader currently {} MB behind ", files.size()-1);
        }
        currentInFile = files.get(0);
        if (eventReader != null) {
          eventReader.close();
          eventReader = null;
        }
      } else {
        if (blocking) {
          waitForWriter();
        } else {
          return false;
        }
      }
    }
    if (eventReader == null) {
      LOGGER.info("Opening New Reader {} ", currentInFile);
      eventReader = new BufferedReader(new FileReader(currentInFile));
      lineNo = 0;
    }
    return true;
  }

  private boolean nextReader(boolean blocking) throws IOException {
    if (eventReader != null) {
      LOGGER.debug("Closing Reader File {} ", currentInFile);
      eventReader.close();
      eventReader = null;
    }
    if (currentInFile != null) {
      if (deleteQueue != null) {
        deleteQueue.add(currentInFile);
        positionFile.delete();
        currentInFile = null;
      } else {
        LOGGER.info("Deleting Reader File {} ", currentInFile);
        currentInFile.delete();
        positionFile.delete();
        currentInFile = null;
      }
    }
    return checkReaderOpen(blocking);
  }

  private void waitForWriter() throws IOException {
    if (running) {
      // just incase we have to wait for a while for the lock, get the last modified now,
      // so we can see if its modified since we started waiting.
      synchronized (waitingForFileLock) {
        try {
          LOGGER.debug("Waiting for more data read:{} written:{} ", nread, nwrite);
          if (nread > nwrite) {
            // reset counters if were catching up
            nread = nwrite;
            // +1 because an event was written which makes nwrite nread+1 when there are none left
          } else if ( nread+1 < nwrite ) {
            LOGGER.debug("Possible event loss, waiting to read when there are more events written read:{} written:{}",nread,nwrite);
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
