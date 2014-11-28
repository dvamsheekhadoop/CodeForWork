package com.wmg;

import java.util.Date;
import java.util.Random;

public class EncryptData {

	private static final char alphas[] = { 'A', 'B', 'C', 'D', 'E', 'F', 'G',
			'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
			'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g',
			'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
			'u', 'v', 'w', 'x', 'y', 'z' };

	public EncryptData() {
	}

	private static int getRandom(int length) {
		return (new Random()).nextInt(length);
	}

	private static char getRandomChar() {
		return alphas[getRandom(alphas.length)];
	}

	public static String evaluate(String strEmail)
    {
        String result = new String();
        if(strEmail == null)
        {
            return null;
        }
        if(strEmail.contains("@"))
        {
            int size = getRandom(25) + getRandom(10);
            StringBuilder sb = new StringBuilder();
            for(int i = 0; i < size; i++)
            {
                sb.append(getRandomChar());
            }

            result = new StringBuilder(String.valueOf(Math.random()))
            		.append(sb.toString())
            		.append(new Date().getTime())
            		.append(strEmail.substring(strEmail.indexOf('@'),strEmail.length()))
            		.toString();
        } else
        {
            result = strEmail;
        }
        return result;
    }

	public static void main(String[] args) {
		System.out.println(evaluate("+Sarath@gmail.com"));
	}
}