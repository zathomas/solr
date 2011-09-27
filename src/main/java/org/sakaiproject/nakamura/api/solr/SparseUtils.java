/**
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
package org.sakaiproject.nakamura.api.solr;

import org.apache.commons.lang.StringUtils;
import org.sakaiproject.nakamura.api.lite.Session;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.content.Content;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SparseUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparseUtils.class);

  public static String getResourceType(RepositorySession repositorySession, String path) {
    String resourceType = null;
    if (repositorySession != null && !StringUtils.isBlank(path)) {
      Session sparseSession = repositorySession.adaptTo(Session.class);
      try {
        Content content = sparseSession.getContentManager().get(path);
        if (content != null) {
          resourceType = (String) content.getProperty("sling:resourceType");
        }
      } catch (StorageClientException e) {
        LOGGER.warn(e.getMessage(), e);
      } catch (AccessDeniedException e) {
        LOGGER.warn(e.getMessage(), e);
      }
    }
    return resourceType;
  }
}
