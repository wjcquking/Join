package org.macau.token;

public class TokenGenerate {

	public static final String TOKEN_SEPARATOR = "\\s+";
	
	
	public static String[] getTokens(String text){
		return text.split(TOKEN_SEPARATOR);
	}
}
