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

import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class SafeSolrMap<K, V> implements Map<K,V> {

  
  private Map<K, V> map;

  public SafeSolrMap(Map<K, V> map) {
    this.map = map;
  }
  public void clear() {
    this.map.clear();
  }

  public boolean containsKey(Object key) {
    return this.map.containsKey(key);
  }

  public boolean containsValue(Object value) {
    for ( K k : map.keySet() ) {
      if ( value == null && map.get(k) == null ) {
        return true;
      }
      if ( map.get(k).equals(value) ) {
        return true;
      }
    }
    return false;
  }

  public Set<java.util.Map.Entry<K, V>> entrySet() {
    Set<K> ks = this.map.keySet();
    @SuppressWarnings("unchecked")
    Entry<K,V>[] elements = new Entry[ks.size()];
    int i = 0;
    for ( K k : ks ) {
      final K kf = k;
      elements[i++] = new Entry<K, V>() {

        public K getKey() {
          return kf;
        }

        public V getValue() {
          return map.get(kf);
        }

        public V setValue(V value) {
          return map.put(kf, value);
        }
      };
    }
    Set<Entry<K,V>> entrySet = ImmutableSet.copyOf(elements);
    return entrySet;
  }

  public V get(Object key) {
    return map.get(key);
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  public Set<K> keySet() {
    return map.keySet();
  }

  public V put(K key, V value) {
    return map.put(key, value);
  }

  public void putAll(Map<? extends K, ? extends V> all) {
    map.putAll(all);
  }

  public V remove(Object key) {
    return map.remove(key);
  }

  public int size() {
    return map.size();
  }

  public Collection<V> values() {
    return map.values();
  }

}
