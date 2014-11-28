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

import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.compression.lzo.LzopInputStream;

public class LzopCryptoCodec extends LzopCodec {
	
	public static final String CRYPTO_ENCRYPTION_BITS = "crypto.codec.encryption.bits";
	public static final String CRYPTO_ENCRYPTION_KEY = "crypto.codec.encryption.key";
	
	public static final String DEFAULT_CRYPTO_LZO_EXTENSION = ".clzo";

	@Override
	  public CompressionOutputStream createOutputStream(OutputStream out,
	          Compressor compressor) throws IOException {
		
		Cipher ecipher = getCipher(Cipher.ENCRYPT_MODE);

		CipherOutputStream cos = new CipherOutputStream(out, ecipher);
		  
	    return createIndexedOutputStream(cos, null, compressor);
	  }

	
	@Override
	public CompressionInputStream createInputStream(InputStream in,
			Decompressor decompressor) throws IOException {
		// Ensure native-lzo library is loaded & initialized
		if (!isNativeLzoLoaded(getConf())) {
			throw new RuntimeException("native-lzo library not available");
		}
		
		Cipher dcipher = getCipher(Cipher.DECRYPT_MODE);

		CipherInputStream cis = new CipherInputStream(in, dcipher);
		
		return new LzopInputStream(cis, decompressor, getConf().getInt(
				LZO_BUFFER_SIZE_KEY, DEFAULT_LZO_BUFFER_SIZE));
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
		return DEFAULT_CRYPTO_LZO_EXTENSION;
	}
}
