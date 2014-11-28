/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jpmc.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import com.hadoop.compression.lzo.LzopCodec;
import com.jpmc.crypto.compression.codec.SnappyCryptoCodec;

public class TestCodec {

	private static final Log LOG = LogFactory.getLog(TestCodec.class);

	private Configuration conf = new Configuration();

	@Test
	public void snappyCryptoCodecTest() throws IOException {
		conf.setInt(SnappyCryptoCodec.CRYPTO_ENCRYPTION_BITS, 128);
		conf.set(SnappyCryptoCodec.CRYPTO_ENCRYPTION_KEY, "testjpmc");
		codecTest(com.jpmc.crypto.compression.codec.SnappyCryptoCodec.class);
	}
	
	@Test
	public void snappyCodecTest() throws IOException {
		conf.setInt(SnappyCryptoCodec.CRYPTO_ENCRYPTION_BITS, 128);
		conf.set(SnappyCryptoCodec.CRYPTO_ENCRYPTION_KEY, "testjpmc");
		codecTest(SnappyCodec.class);
	}
	
	@Test
	public void LzopCryptoCodecTest() throws IOException {
		conf.setInt(SnappyCryptoCodec.CRYPTO_ENCRYPTION_BITS, 128);
		conf.set(SnappyCryptoCodec.CRYPTO_ENCRYPTION_KEY, "testjpmc");
		codecTest(com.jpmc.crypto.compression.codec.LzopCryptoCodec.class);
	}
	
	@Test
	public void LzopCodecTest() throws IOException {
		conf.setInt(SnappyCryptoCodec.CRYPTO_ENCRYPTION_BITS, 128);
		conf.set(SnappyCryptoCodec.CRYPTO_ENCRYPTION_KEY, "testjpmc");
		codecTest(LzopCodec.class);
	}
	
	@Test
	public void defaultCodecTest() throws IOException {
		conf.setInt(SnappyCryptoCodec.CRYPTO_ENCRYPTION_BITS, 128);
		conf.set(SnappyCryptoCodec.CRYPTO_ENCRYPTION_KEY, "testjpmc");
		codecTest(DefaultCodec.class);
	}

	private void codecTest(Class<? extends CompressionCodec> codecClass)
			throws IOException {

		// Create the codec
		CompressionCodec codec = null;
		codec = (CompressionCodec) ReflectionUtils
				.newInstance(codecClass, conf);
		LOG.info("Created a Codec object of type: " + codecClass);

		// Compress data
		int read = 0;
		byte[] bytes = new byte[1024];

		FileInputStream sourceIn = new FileInputStream(
				"src/test/resources/original_sample.txt");

		FileOutputStream fo = new FileOutputStream(
				"src/test/resources/compressed_sample"+codec.getDefaultExtension());
		CompressionOutputStream compressedOutStream = codec
				.createOutputStream(fo);
		while ((read = sourceIn.read(bytes)) != -1) {
			compressedOutStream.write(bytes, 0, read);
		}
		compressedOutStream.close();
		sourceIn.close();

		LOG.info("Finished compressing data");

		FileOutputStream fout = new FileOutputStream(
				"src/test/resources/uncompressed_sample"+codec.getDefaultExtension()+".txt");

		FileInputStream compressedIn = new FileInputStream(
				"src/test/resources/compressed_sample"+codec.getDefaultExtension());
		CompressionInputStream compressedInStream = codec
				.createInputStream(compressedIn);
		while ((read = compressedInStream.read(bytes)) != -1) {
			fout.write(bytes, 0, read);
		}
		fout.close();
		compressedInStream.close();
		Assert.assertTrue(FileUtils.contentEquals(new File(
				"src/test/resources/uncompressed_sample"+codec.getDefaultExtension()+".txt"), new File(
				"src/test/resources/original_sample.txt")));
	}
}
