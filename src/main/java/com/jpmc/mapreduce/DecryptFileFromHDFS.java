package com.jpmc.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.security.InvalidKeyException;
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

public class DecryptFileFromHDFS {

	private static final String SECRETKEY = "testjpmc";
	private static final int pswdIterations = 16384;
	private static final int keySize = 128;

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			usage();
		}
		Configuration conf = new Configuration();

		FileSystem hdfs = FileSystem.get(new URI(args[0]), conf);
		FileSystem localFS = FileSystem.get(new URI(args[1]), conf);

		Path inFile = new Path(args[0]);
		Path outFile = new Path(args[1]);

		if (!hdfs.exists(inFile))
			printAndExit("Input file not found");
		if (!hdfs.isFile(inFile))
			printAndExit("Input should be a file");
		if (localFS.exists(outFile))
			printAndExit("Output already exists");

		FSDataInputStream in = hdfs.open(inFile);
		FSDataOutputStream out = localFS.create(outFile);

		CipherOutputStream cos = new CipherOutputStream(out,
				getCipher(Cipher.DECRYPT_MODE));
		byte[] buffer = new byte[256];
		try {
			int bytesRead = 0;
			while ((bytesRead = in.read(buffer)) != -1) {
				cos.write(buffer, 0, bytesRead);
			}
		} catch (IOException e) {
			System.out.println("Error while decrypting and writing the file.");
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
				.println("Usage : DecryptFileFromHDFS <hdfsinputfile> <localoutputfile>");
		System.exit(1);
	}

	static Cipher getCipher(int mode) throws NoSuchAlgorithmException,
			NoSuchPaddingException, InvalidKeyException,
			UnsupportedEncodingException, InvalidKeySpecException {
		// Derive the key
		SecretKeyFactory factory = SecretKeyFactory
				.getInstance("PBKDF2WithHmacSHA1");
		PBEKeySpec spec = new PBEKeySpec(SECRETKEY.toCharArray(),
				SECRETKEY.getBytes(), pswdIterations, keySize);

		SecretKey secretKey = factory.generateSecret(spec);
		SecretKeySpec secret = new SecretKeySpec(secretKey.getEncoded(), "AES");

		Cipher cipher = Cipher.getInstance("AES");
		cipher.init(mode, secret);

		return cipher;
	}
}
