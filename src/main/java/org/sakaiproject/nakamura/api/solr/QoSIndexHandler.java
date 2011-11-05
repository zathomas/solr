package org.sakaiproject.nakamura.api.solr;

import org.osgi.service.event.Event;

/**
 * IndexHandlers should implement this method if they want to specify a TTL on
 * certain types of event. Only index handlers that really need to do this
 * should do so, and they should base their decision on the event and its
 * properties, avoiding accessing the content system, as this call may be made
 * before the transaction thats writing to the item is complete.
 * 
 * @author ieb
 * 
 */
public interface QoSIndexHandler {

	/**
	 * @param event
	 * @return the TTL or one of 0 or Iteger.MAX_VALUE if the handler has no TTL
	 *         that needs to be specified.
	 */
	int getTtl(Event event);

}
