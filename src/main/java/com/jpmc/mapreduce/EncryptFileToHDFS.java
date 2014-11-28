package com.jpmc.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class EncryptFileToHDFS {

	private static final String SECRETKEY = "testjpmc";
	private static final int pswdIterations = 1000;
	private static final int keySize = 256;
	
	  public static final String CIPHER_ALGORITHM = "AES";
	  public static final String KEY_ALGORITHM = "AES";
	  public static final String PASSWORD_HASH_ALGORITHM = "SHA-256";

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			usage();
		}

		Configuration conf = new Configuration();

		FileSystem hdfs = FileSystem.get(new URI(args[1]), conf);
		FileSystem localFS = FileSystem.get(new URI(args[0]), conf);

		Path inFile = new Path(args[0]);
		Path outFile = new Path(args[1]);

		if (!localFS.exists(inFile))
			printAndExit("Input file not found");
		if (!localFS.isFile(inFile))
			printAndExit("Input should be a file");
		if (hdfs.exists(outFile))
			printAndExit("Output already exists");

		Cipher cipher = getCipher(Cipher.ENCRYPT_MODE);

		FSDataInputStream in = localFS.open(inFile);
		FSDataOutputStream out = hdfs.create(outFile);

		CipherOutputStream cos = new CipherOutputStream(out, cipher);
		
		byte[] buffer = new byte[256];
		try {
			int bytesRead = 0;
			while ((bytesRead = in.read(buffer)) != -1) {
				cos.write(buffer, 0, bytesRead);
			}
		} catch (IOException e) {
			System.out.println("Error while encrypting and writing the file.");
		} finally {
			in.close();
			cos.close();
		}
	}

	static void printAndExit(String str) {
		System.err.println(str);
		System.exit(1);
	}

	static void usage() {
		System.out
				.println("Usage : EncryptFileToHDFS <localinputfile> <hdfsoutputfile>");
		System.exit(1);
	}

	static Cipher getCipher(int mode) throws NoSuchAlgorithmException,
			NoSuchPaddingException, InvalidKeyException,
			UnsupportedEncodingException, InvalidKeySpecException {
		
		String key = "av45k1pfb024xa3bav45k1pfb024xa3b";//l359vsb4esortvks74sksr5oy4s5serondry84jsrryuhsr5ys49y5seri5shrdliheuirdygliurguiy5ruav45k1pfb024xa3bl359vsb4esortvks74sksr5oy4s5serondry84jsrryuhsr5ys49y5seri5shrdliheuirdygliurguiy5rutvks74sksrtvks74sksrtvks74sksrtvks74sksrtvks74sksrwertyu";

		// Derive the key
		SecretKeyFactory factory = SecretKeyFactory
				.getInstance("PBKDF2WithHmacSHA1");
		PBEKeySpec spec = new PBEKeySpec(SECRETKEY.toCharArray(), SECRETKEY.getBytes(),
				pswdIterations, keySize);

		SecretKey secretKey = factory.generateSecret(spec);
		Key secret = new SecretKeySpec(secretKey.getEncoded(), "AES"); //buildKey(SECRETKEY);
		

		Cipher cipher = Cipher.getInstance("AES");
		cipher.init(mode, secret);

		return cipher;
	}
	
	private static Key buildKey(String password) throws NoSuchAlgorithmException, UnsupportedEncodingException {
	    MessageDigest digester = MessageDigest.getInstance(PASSWORD_HASH_ALGORITHM);
	    digester.update(password.getBytes("UTF-8"));
	    byte[] key = digester.digest();
	    SecretKeySpec spec = new SecretKeySpec(key, KEY_ALGORITHM);
	    return spec;
	  }
}
