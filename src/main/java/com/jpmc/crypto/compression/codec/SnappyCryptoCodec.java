package com.jpmc.crypto.compression.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.Key;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.compress.BlockCompressorStream;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SnappyCodec;

public class SnappyCryptoCodec extends SnappyCodec {
	
	public static final String CRYPTO_ENCRYPTION_BITS = "crypto.codec.encryption.bits";
	public static final String CRYPTO_ENCRYPTION_KEY = "crypto.codec.encryption.key";
	
	public static final String DEFAULT_CRYPTO_SNAPPY_EXTENSION = ".csnappy";

		
	@Override
	public CompressionOutputStream createOutputStream(OutputStream out,
			Compressor compressor) throws IOException {
		checkNativeCodeLoaded();
		Cipher cipher = getCipher(Cipher.ENCRYPT_MODE);

		CipherOutputStream cos = new CipherOutputStream(out, cipher);

		int bufferSize = getConf().getInt(
		        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY,
		        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT);

		
		int compressionOverhead = (bufferSize / 6) + 32;

		return new BlockCompressorStream(cos, compressor, bufferSize,
				compressionOverhead);
	}
	
	
	  /**
	   * Create a {@link CompressionInputStream} that will read from the given
	   * {@link InputStream} with the given {@link Decompressor}.
	   *
	   * @param in           the stream to read compressed bytes from
	   * @param decompressor decompressor to use
	   * @return a stream to read uncompressed bytes from
	   * @throws IOException
	   */
	  @Override
	  public CompressionInputStream createInputStream(InputStream in,
	                                                  Decompressor decompressor)
	      throws IOException {
	    checkNativeCodeLoaded();
	    
	    Cipher cipher = getCipher(Cipher.DECRYPT_MODE);

		CipherInputStream cis = new CipherInputStream(in, cipher);
	    
	    return new BlockDecompressorStream(cis, decompressor, getConf().getInt(
	        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY,
	        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT));
	  }

	
		private Cipher getCipher(int mode) {
			Cipher cipher = null;
			try {
				// Derive the key
				int keySize = getConf().getInt(CRYPTO_ENCRYPTION_BITS, 128);
				int pswdIterations = 1000;
				String SECRETKEY = getConf().get(CRYPTO_ENCRYPTION_KEY);
				SecretKeyFactory factory = SecretKeyFactory
						.getInstance("PBKDF2WithHmacSHA1");
				PBEKeySpec spec = new PBEKeySpec(SECRETKEY.toCharArray(),
						SECRETKEY.getBytes(), pswdIterations, keySize);

				SecretKey secretKey = factory.generateSecret(spec);
				Key secret = new SecretKeySpec(secretKey.getEncoded(), "AES"); // buildKey(SECRETKEY);

				cipher = Cipher.getInstance("AES");
				cipher.init(mode, secret);
			} catch (Exception e) {
				e.printStackTrace();
			}

			return cipher;
		}
		
		@Override
		public String getDefaultExtension() {
			return DEFAULT_CRYPTO_SNAPPY_EXTENSION;
		}
}
