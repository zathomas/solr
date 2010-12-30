package org.sakaiproject.nakamura.api.solr;

public interface RepositorySession {
    <T> T adaptTo(Class<T> c);
}
