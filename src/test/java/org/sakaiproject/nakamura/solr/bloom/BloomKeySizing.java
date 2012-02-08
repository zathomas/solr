package org.sakaiproject.nakamura.solr.bloom;

import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;
import java.util.Random;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.bloom.RetouchedBloomFilter;
import org.apache.hadoop.util.hash.Hash;
import org.junit.Test;

public class BloomKeySizing {

	public class PositiveNegative {

		public int nkeys;
		public int keySize;
		public int keysInFilter;
		public int filterBits;
		public int nFilterHashes;
		public int falseNegative;
		public int falsePositive;
		public int ntests;
		public int nchecks;
		public int truePositive;
		public int trueNegative;

		public PositiveNegative(int nkeys, int keysInFilter, int keySize,
				int filterBits, int nFilterHashes, int ntests) {
			this.nkeys = nkeys;
			this.keySize = keySize;
			this.keysInFilter = keysInFilter;
			this.filterBits = filterBits;
			this.nFilterHashes = nFilterHashes;
			this.ntests = ntests;
		}

		public PositiveNegative() {
			// TODO Auto-generated constructor stub
		}

		@Override
		public String toString() {
			int truePositiveTarget = keysInFilter * ntests;
			int trueNegativeTarget = (nkeys - keysInFilter) * ntests;
			int falsePositiveRate = (100 * falsePositive) / (nkeys * ntests);
			int falseNegativeRate = (100 * falseNegative) / (nkeys * ntests);

			return csvLine(nkeys, filterBits, keysInFilter, keySize,
					nFilterHashes, truePositive, truePositiveTarget,
					trueNegative, trueNegativeTarget, falsePositive,
					falsePositiveRate, falseNegative, falseNegativeRate,
					nchecks, ntests);
		}

		private String csvLine(Object... cs) {
			StringBuilder sb = new StringBuilder();
			for (Object c : cs) {
				if (c instanceof Integer) {
					sb.append(c).append(",");
				} else {
					sb.append("\"").append(c).append("\",");
				}
			}
			return sb.toString();
		}

		public String header() {
			return csvLine("Number of Keys", "Bloom Filter Bits",
					"Keys In Filter", "Key Size in bytes", "Number of Hashes",
					"True Positives", "True Positives Target",
					"True Negatives", "True Negatives Target",
					"False Positives", "False Positives Rate",
					"False Negatives", "False Negatives Rate",
					"Number of Checks", "Number of Tests");
		}

		public void printIfErrors() {
			if ((falseNegative + falsePositive) > 0) {
				System.err.println(toString());
			}
		}

		public boolean isAcceptable() {
			int falsePositiveRate = (100 * falsePositive) / (nkeys * ntests);
			int falseNegativeRate = (100 * falseNegative) / (nkeys * ntests);
			return (falseNegativeRate >= 0) && (falsePositiveRate >= 0) && (falseNegativeRate < 20) && (falsePositiveRate < 20);
		}

	}

	@Test
	public void testBloom() throws UnsupportedEncodingException {
		System.err.println(new PositiveNegative().header());
		for (int i = 2048; i < 10000001; i*=2) {
			for ( int j = 2; j < 1025; j*=2) {
				int accepted = 0;
				for(int k = 2; k < 256; k*=2) {
					PositiveNegative test = new PositiveNegative(i, j, 20, k*8, 5, 100);
					doTest(test)
							.printIfErrors();
					if ( test.isAcceptable() ) {
						accepted = k;
						break;
					}
				}
				if ( accepted > 0 ) {
					System.err.println("\"Acceptable\",\"n principals total\","+i+",\"n user principals\","+j+",\"bloom bits\","+(accepted*8));
				} else {
					System.err.println("\"Not Acceptable\",\"n principals total\","+i+",\"n user principals\","+j+",\"bloom bits\","+(2048));
				}
			}
		}
	}

	public PositiveNegative doTest(PositiveNegative testSetup) {
		Random r = new Random(0xdeadbeef);
		Key[] keys = new Key[testSetup.nkeys];
		int[] keyinfilter = new int[testSetup.keysInFilter]; // index of the
																// contents of
																// the bloom
																// filter
		int[] infilter = new int[testSetup.nkeys];
		for (int i = 0; i < keys.length; i++) {
			byte[] b = new byte[testSetup.keySize];
			r.nextBytes(b);
			keys[i] = new Key(b);
		}

		for (int test = 0; test < testSetup.ntests; test++) {
			BloomFilter bf = new BloomFilter(testSetup.filterBits,
					testSetup.nFilterHashes, Hash.JENKINS_HASH);
			for (int i = 0; i < infilter.length; i++) {
				infilter[i] = 0;
			}
			for (int i = 0; i < keyinfilter.length; i++) {
				while (true) {
					keyinfilter[i] = r.nextInt(keys.length);
					if (infilter[keyinfilter[i]] == 0) {
						bf.add(keys[keyinfilter[i]]);
						infilter[keyinfilter[i]] = 1;
						break;
					}
				}
			}

			for (int i = 0; i < keys.length; i++) {
				boolean match = bf.membershipTest(keys[i]);
				testSetup.nchecks++;
				if (infilter[i] == 1) {
					if (!match) {
						testSetup.falseNegative++;
					} else {
						testSetup.truePositive++;
					}
				} else {
					if (match) {
						testSetup.falsePositive++;
					} else {
						testSetup.trueNegative++;
					}

				}
			}
		}
		return testSetup;
	}

}
