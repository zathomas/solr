package org.sakaiproject.nakamura.solr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.UpdateParams;
import org.osgi.service.event.Event;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.solr.IndexingHandler;
import org.sakaiproject.nakamura.api.solr.RepositorySession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class QueueManager implements Runnable {

	private static final String END = "--end--";

	private static final Logger LOGGER = LoggerFactory
			.getLogger(QueueManager.class);
	private File logDirectory;
	private File positionFile;
	private Set<File> deleteQueue;
	private File savedCurrentInFile;
	private int savedLineNo;
	private long batchStart;
	private boolean nearRealTime;
	private long nread;
	private long nwrite;
	private BufferedReader eventReader;
	private File currentInFile;
	private Object waitingForFileLock = new Object();
	private boolean running = false;
	private int lineNo;
	private File currentFile;
	private FileWriter eventWriter;
	protected int batchedIndexSize;
	protected long batchDelay;

	private Thread queueDispatcher;

	private QueueManagerDriver queueManagerDriver;

	private String queueName;

	private Object eventWriterSync = new Object();

	public QueueManager(QueueManagerDriver queueManagerDriver,
			String queueHome, String queueName, boolean nearRealTime,
			int batchedIndexSize, long batchDelay) throws IOException {
		if (queueName == null || queueName.equals("")) {
			this.queueName = "";
		} else {
			this.queueName = "-" + queueName;
		}
		logDirectory = new File(queueHome, "indexq" + this.queueName);
		positionFile = new File(queueHome, "indexqpos" + this.queueName);
		if (!logDirectory.isDirectory()) {
			if (!logDirectory.mkdirs()) {
				LOGGER.warn("Failed to create {} ",
						logDirectory.getAbsolutePath());
				throw new IOException("Faild to create Indexing Log directory "
						+ logDirectory.getAbsolutePath());
			}
		}
		this.nearRealTime = nearRealTime;
		this.batchDelay = batchDelay;
		this.batchedIndexSize = batchedIndexSize;
		this.queueManagerDriver = queueManagerDriver;
		loadPosition();
		running = false;
	}

	public synchronized void start() {
		if (!running) {
			queueDispatcher = new Thread(this);
			queueDispatcher.setName("IndexQueueManager" + queueName);
			queueDispatcher.start();
			running = true;
		} else {
			LOGGER.warn("QueueManager {} already running ",queueName);
		}
	}

	public synchronized void stop() throws IOException {
		if (running) {
			closeWriter();
			running = false;
			notifyReader();
		}

	}
	
	public synchronized boolean isRunning() {
		return running;
	}

	public void run() {
		LOGGER.info("Queue Manager Starting {} ",queueName);
		batchedEventRun();
	}

	public Thread getQueueDispatcher() {
		return queueDispatcher;
	}

	public void closeWriter() throws IOException {
		boolean notify = false;
		synchronized (eventWriterSync) {
			if (eventWriter != null) {
				LOGGER.debug("Writer closing {} ", currentFile.getName());
				nwrite++;
				eventWriter.append(END);
				eventWriter.flush();
				eventWriter.close();
				eventWriter = null;
				currentFile = null;
				notify = true;
			}
		}
		if (notify) {
			notifyReader();
		}
	}

	public void saveEvent(Event event) throws IOException {
		synchronized (eventWriterSync) {
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
				currentFile = new File(logDirectory, String.valueOf(System
						.currentTimeMillis()));
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
				op[i] = URLEncoder.encode(String.valueOf(event.getProperty(p)),
						"UTF8");
				i++;
			}
			eventWriter.append(StringUtils.join(op, ',')).append('\n');
			nwrite++;
			eventWriter.flush();
		}
		notifyReader();
	}


	private void fireCommitEvent(String topic) {
		Dictionary<String, Object> props = new Hashtable<String, Object>();

		queueManagerDriver
			.getEventAdmin()
			.postEvent(new Event(topic, props));
	}


	private void batchedEventRun() {
		int backoff = 0;
		while (running) {
			RepositorySession repositorySession = null;
			try {
				begin();
				Event loadEvent = null;
				try {
					loadEvent = readEvent();
				} catch (Throwable t) {
					if (running) {
						LOGGER.warn("Unreadable Event at {} {} ",
								currentInFile, lineNo);
						LOGGER.warn("Reported exception follows:", t);
					} else {
						LOGGER.debug("Unreadable Event at {} {} ",
								currentInFile, lineNo);
						LOGGER.debug("Reported exception follows:", t);
					}
				}
				Map<String, Event> events = Maps.newLinkedHashMap();
				while (loadEvent != null) {
					String topic = loadEvent.getTopic();
					String path = (String) loadEvent.getProperty("path");
					if (path != null) {
						Collection<IndexingHandler> contentIndexHandler = queueManagerDriver
								.getTopicHandler(topic);
						if (contentIndexHandler != null
								&& contentIndexHandler.size() > 0) {
							if (events.containsKey(path)) {
								// events is a linked hash map this will put it
								// at the end.
								events.remove(path);
							}
							events.put(path, loadEvent);
						}
					} else {
						LOGGER.info(
								"Ignoring event [{}] because it lacks a 'path' property {}",
								loadEvent,
								Arrays.toString(loadEvent.getPropertyNames()));
					}
					if (events.size() >= batchedIndexSize) {
						break;
					}

					loadEvent = null;
					try {
						loadEvent = readEvent();
					} catch (Throwable t) {
						LOGGER.warn("Unreadable Event at {} {} ",
								currentInFile, lineNo);
						LOGGER.warn("Reported exception follows:", t);
					}
				}
				if (events.size() > 0) {
					LOGGER.info(
							"Processing a batch of {} items, redolog at {}:{}, time remaining for this batch {}",
							new Object[] { events.size(), currentInFile,
									lineNo, getBatchTTL() });
				}
				SolrServer service = queueManagerDriver.getSolrServerService()
						.getUpdateServer();
				try {
					boolean needsCommit = false;
					for (Entry<String, Event> ev : events.entrySet()) {
						Event event = ev.getValue();
						String topic = event.getTopic();
						Collection<IndexingHandler> contentIndexHandlers = queueManagerDriver
								.getTopicHandler(topic);
						if (contentIndexHandlers != null) {
							for (IndexingHandler contentIndexHandler : contentIndexHandlers) {
								/**
								 * getDeleteQueries and getDocuments must be
								 * called for all registered indexing handlers.
								 * There is the chance that updating a document
								 * will cause another document to be deleted and
								 * this is the only way the indexing handler has
								 * to interact in that. e.g. sakai:excludeSearch
								 * gets set to true; that doc needs to be
								 * deleted.
								 */
								String path = "undefined";
								Collection<SolrInputDocument> docs = null;
								try {
									path = (String) event.getProperty("path");
									LOGGER.debug(
											"Got Handler {} for event {} {}",
											new Object[] { contentIndexHandler,
													event, path });
									if ( repositorySession == null ) {
										repositorySession = getRepositorySession();
									}
									for (String deleteQuery : contentIndexHandler
											.getDeleteQueries(
													repositorySession, event)) {
										if (service != null) {
											LOGGER.debug(
													"Added delete Query {} ",
													deleteQuery);
											try {
												service.deleteByQuery(deleteQuery);
												needsCommit = true;
											} catch (SolrServerException e) {
												LOGGER.info(
														" Failed to delete {}  cause :{}",
														deleteQuery,
														e.getMessage());
											}
										}
									}
									docs = contentIndexHandler.getDocuments(
											repositorySession, event);

									if (service != null) {
										if (docs != null && docs.size() > 0) {
											LOGGER.debug("Adding Docs {} ",
													docs);
											service.add(docs);
											needsCommit = true;
										}
									}
								} catch (Throwable t) {
									 if ( t instanceof SolrServerException && t.getCause() instanceof ConnectException ) {
										throw (SolrServerException)t;
								     }
									 LOGGER.error(
											"{} Failed to process event {}, {} cause follows, event ignored for "
													+ "this processor, please fix issue to remove this message (dont delete "
													+ "this log message from the code) ",
											new Object[] { contentIndexHandler,
													event, path });
									LOGGER.error(t.getMessage(), t);
									if (docs != null) {
										for (SolrInputDocument d : docs) {
											LOGGER.error("Failed Doc {} ", d);
										}
									}
								}
							}
						}
					}
					if (needsCommit) {
						LOGGER.info(
								"Processed {} events in a batch, max {}, TTL {}, queue at {}:{}  ",
								new Object[] { events.size(), batchedIndexSize,
										getBatchTTL(), currentFile, lineNo});
						if (nearRealTime) {
							UpdateRequest updateRequest = new UpdateRequest();
							updateRequest.setAction(
									AbstractUpdateRequest.ACTION.COMMIT, false,
									false);
							updateRequest.setParam(UpdateParams.SOFT_COMMIT,
									"true");
							updateRequest.process(service);
							fireCommitEvent("org/sakaiproject/nakamura/solr/SOFT_COMMIT");

						} else {
							service.commit(false, false);
							fireCommitEvent("org/sakaiproject/nakamura/solr/COMMIT");
						}
					}
					backoff = 0;
					commit();
				} catch (SolrServerException e) {
					if (e.getCause() instanceof ConnectException) {
						if (backoff == 0) {
							backoff = 2;
						} else if (backoff > 60) {
							backoff = 2;
						} else {
							backoff = backoff * 2;
						}
						LOGGER.warn(
								"Remote Solr master is down, will retry index operation in {}s ",
								backoff);

						rollback();
						Thread.sleep(1000 * backoff);
					} else {
						LOGGER.warn(
								" Batch Operation completed with Errors, the index may have lost data, please FIX ASAP. "
										+ e.getMessage(), e);
						try {
							service.rollback();
						} catch (Exception e1) {
							LOGGER.warn(e.getMessage(), e1);
						}
			            backoff = 0;
						commit();
					}
				} catch (IOException e) {
					LOGGER.warn(e.getMessage(), e);
		            backoff = 0;
					rollback();
				} catch (SolrException e) {
					LOGGER.warn(e.getMessage(), e);
		            backoff = 0;
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
			} finally {
				try {
					if ( repositorySession != null ) {
						repositorySession.logout();
					}
				} catch (Exception e) {
					LOGGER.warn(e.getMessage(), e);
				}
			}
		}
		LOGGER.info("QueueManager {} shutting down ",queueName);
	}

	private RepositorySession getRepositorySession()
			throws ClientPoolException,
			StorageClientException, AccessDeniedException {
		final org.sakaiproject.nakamura.api.lite.Session sparseSession = queueManagerDriver
				.getSparseRepository().loginAdministrative();
		return new RepositorySession() {

			@SuppressWarnings("unchecked")
			public <T> T adaptTo(Class<T> c) {
				if (c.equals(org.sakaiproject.nakamura.api.lite.Session.class)) {
					return (T) sparseSession;
				}
				return null;
			}

			public void logout() {
				try {
					sparseSession.logout();
				} catch (Exception e) {
					LOGGER.warn(e.getMessage(), e);
				}
			}
		};
	}

	private void begin() {
		savedCurrentInFile = currentInFile;
		savedLineNo = lineNo;
		deleteQueue = Sets.newHashSet();
		batchStart = System.currentTimeMillis();
	}

	private void commit() throws IOException {
		if (deleteQueue != null) {
			for (File f : deleteQueue) {
				LOGGER.info("Deleting Reader File {} ", f);
				if (!f.delete()) {
					LOGGER.debug(
							"Failed to delete Redo file, {} might be an issue",
							f);
				}
			}
			if (!positionFile.delete()) {
				LOGGER.debug(
						"Failed to delete Possition file, {} might be an issue",
						positionFile);
			}
			deleteQueue.clear();
			deleteQueue = null;
		}
		if (currentInFile != null) {
			savePosition();
		}
	}

	private void rollback() throws IOException {
		currentInFile = savedCurrentInFile;
		lineNo = savedLineNo;
		deleteQueue = null;
		savePosition();
		if (eventReader != null) {
			eventReader.close();
			eventReader = null;
		}
		loadPosition(); // reopen the event reader to reset its position.
		LOGGER.info("Rolled back queue to {}:{} ",currentFile,lineNo);
	}

	private void savePosition() throws IOException {
		if (currentInFile == null) {
			if (!positionFile.delete()) {
				LOGGER.debug(
						"Failed to delete Possition file, {} might be an issue",
						positionFile);
			}
		} else {
			FileWriter position = new FileWriter(positionFile);
			position.write(URLEncoder.encode(currentInFile.getAbsolutePath(),
					"UTF8"));
			position.write(",");
			position.write(String.valueOf(lineNo));
			position.write("\n");
			position.close();
		}
	}

	private void loadPosition() throws IOException {
		if (positionFile.exists()) {
			BufferedReader position = null;
			try {
				position = new BufferedReader(new FileReader(positionFile));
				String[] filePos = StringUtils.split(position.readLine(), ',');
				if (filePos != null && filePos.length == 2) {
					currentInFile = new File(URLDecoder.decode(filePos[0],
							"UTF8"));
					if (currentInFile.exists()) {
						lineNo = Integer.parseInt(filePos[1]);
						loadPosition(currentInFile, lineNo);
						return;
					}
				}
			} finally {
				try {
					position.close();
				} catch (IOException e) {
					LOGGER.debug("Failed to close {} ", positionFile);
				}
			}
		}
		currentInFile = null;
		currentFile = null;
		lineNo = 0;
	}

	private Event readEvent() throws IOException {
		if (getBatchTTL() > 0) {
			String line = nextEvent();
			if (line != null) {
				String[] parts = StringUtils.split(line, ',');
				if (parts.length > 0) {
					Dictionary<String, Object> dict = new Hashtable<String, Object>();
					for (int i = 1; i < parts.length; i += 2) {
						dict.put(URLDecoder.decode(parts[i], "UTF8"),
								URLDecoder.decode(parts[i + 1], "UTF8"));
					}
					return new Event(URLDecoder.decode(parts[0], "UTF8"), dict);
				}
			}
		}
		return null;
	}

	private String nextEvent() throws IOException {
		String line = null;
		int possibleEnd = 0;
		if (checkReaderOpen()) {
			while (line == null || END.equals(line)) {
				if (END.equals(line)) {
					LOGGER.debug("At End of file {}", currentInFile);
					if (!nextReader()) {
						return null;
					}
				}
				line = eventReader.readLine();

				if (line != null) {
					possibleEnd = 0;
					nread++;
					lineNo++;
					if ((nread % 10000) == 0) {
						LOGGER.info("Event Redo Log has processed {} events",
								nread);
					}
				} else {
					// if we get null from a buffered reader that means end of
					// file, but there was
					// no end statement
					// so we need to check if this really is the end of file
					if (possibleEnd != 0 || getBatchTTL() > 0) {
						if (possibleEnd == 0) {
							// even though the writer wrote something, we still
							// couldnt read
							waitForWriter();
							possibleEnd = 1;
						} else if (possibleEnd == 1) {
							// even though the writer wrote something, we still
							// couldnt read
							waitForWriter();
							possibleEnd = 2;
						} else if (possibleEnd == 2) {
							// we waited, we reloaded and its still not giving
							// more, all I can assume is
							// the file got closed without a new one, so go for
							// the next reader
							LOGGER.debug(
									"Searching for next file, currently {}",
									currentInFile);
							// one of 2 things can happen here.
							// the file gets appended to or a new file appears.
							List<File> files = Lists.newArrayList(logDirectory
									.listFiles());
							if (deleteQueue != null) {
								files.removeAll(deleteQueue);
							}
							File nextFile = null;

							for (File f : files) {
								if (f.lastModified() > currentInFile
										.lastModified()) {
									if (nextFile == null) {
										nextFile = f;
									} else if (f.lastModified() < nextFile
											.lastModified()) {
										nextFile = f;
									}
								}
							}

							LOGGER.debug("Located next file: {}", nextFile);

							if (nextFile == null) {
								return null;
							} else if (nextFile.equals(currentInFile)) {
								waitForWriter();
								possibleEnd = 4; // try once more
							} else {
								// a new file, try and open that
								nextReader();
								possibleEnd = 4;
							}
						} else if (possibleEnd == 4) {
							// no more events, flush and start next loop
							return null;
						} else {
							waitForWriter();
							return null;
						}
					} else {
						return null;
					}
				}
			}
		}
		return line;
	}

	private boolean nextReader() throws IOException {
		if (eventReader != null) {
			LOGGER.debug("Closing Reader File {} ", currentInFile);
			eventReader.close();
			eventReader = null;
		}
		if (currentInFile != null) {
			if (deleteQueue != null) {
				deleteQueue.add(currentInFile);
				if (!positionFile.delete()) {
					LOGGER.debug(
							"Failed to delete Position file, {} might be an issue",
							positionFile);
				}
				currentInFile = null;
			} else {
				LOGGER.info("Deleting Reader File {} ", currentInFile);
				if (!currentInFile.delete()) {
					LOGGER.debug(
							"Failed to delete Redo file, {} might be an issue",
							currentInFile);
				}
				if (!positionFile.delete()) {
					LOGGER.debug(
							"Failed to delete Position file, {} might be an issue",
							positionFile);
				}
				currentInFile = null;
			}
		}
		return checkReaderOpen();
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

	private boolean checkReaderOpen() throws IOException {
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
				if (files.size() > 1) {
					LOGGER.info("Reader currently {} MB behind ",
							files.size() - 1);
				}
				currentInFile = files.get(0);
				if (eventReader != null) {
					eventReader.close();
					eventReader = null;
				}
			} else {
				if (getBatchTTL() > 0) {
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

	private void waitForWriter() throws IOException {
		if (isRunning()) {
			// just incase we have to wait for a while for the lock, get the
			// last modified now,
			// so we can see if its modified since we started waiting.
			if (getBatchTTL() > 0) {
				synchronized (waitingForFileLock) {
					try {
						LOGGER.debug(
								"Waiting for more data read:{} written:{} ",
								nread, nwrite);
						if (nread > nwrite) {
							// reset counters if were catching up
							nread = nwrite;
							// +1 because an event was written which makes
							// nwrite nread+1 when there are
							// none left
						} else if (nread + 1 < nwrite) {
							LOGGER.debug(
									"Possible event loss, waiting to read when there are more events written read:{} written:{}",
									nread, nwrite);
						}
						long wait = getBatchTTL();
						if (wait > 0) {
							waitingForFileLock.wait(wait);
						}
					} catch (InterruptedException e) {
					}
				}
			}
		}
		if (!isRunning()) {
			throw new IOException(
					"Shutdown in pogress, aborting index queue reader");
		}
	}

	private long getBatchTTL() {
		return batchDelay - (System.currentTimeMillis() - batchStart);
	}

	private void notifyReader() {
		synchronized (waitingForFileLock) {
			waitingForFileLock.notify();
		}
	}

}
