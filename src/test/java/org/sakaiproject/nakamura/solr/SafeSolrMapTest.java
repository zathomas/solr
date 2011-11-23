package org.sakaiproject.nakamura.solr;

import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.sakaiproject.nakamura.api.solr.SafeSolrMap;

import com.google.common.collect.Maps;

public class SafeSolrMapTest {

	@Test
	public void test() {
		Map<String, String> testmap = Maps.newHashMap();
		testmap.put("test", "value");
		SafeSolrMap<String, String> smap = new SafeSolrMap<String, String>(testmap);
		Assert.assertTrue(smap.containsKey("test"));
		Assert.assertFalse(smap.containsKey("ntest"));
		Assert.assertEquals("value",smap.get("test"));
		Assert.assertTrue(smap.containsValue("value"));
		Assert.assertFalse(smap.containsValue("nvalue"));
		Assert.assertTrue(smap.entrySet().size() == 1);
		Assert.assertTrue(smap.size() == 1);
		Assert.assertEquals("test",smap.entrySet().iterator().next().getKey());
		Assert.assertEquals("value",smap.entrySet().iterator().next().getValue());
		Assert.assertEquals("test",smap.keySet().iterator().next());
		Assert.assertEquals("value",smap.values().iterator().next());
		Assert.assertFalse(smap.isEmpty());
		smap.put("test2", "value2");
		Assert.assertTrue(smap.size() == 2);
		smap.remove("test2");
		Assert.assertTrue(smap.size() == 1);
		smap.clear();
		Assert.assertTrue(smap.size() == 0);
		Assert.assertTrue(smap.isEmpty());
		Map<String, String> testmap2 = Maps.newHashMap();
		testmap2.put("test", "value");
		smap.putAll(testmap2);
		smap.put("test2", null);
		Assert.assertNull(smap.get("test2"));
		Assert.assertTrue(smap.containsValue(null));
	}
}
