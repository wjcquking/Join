package org.macau.util;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class SystemUtil {
	private static final Pattern rePunctuation = Pattern
            .compile("[^\\p{L}\\p{N}]"); // L:Letter, N:Number
    private static final Pattern reSpaceOrAnderscore = Pattern
            .compile("(_|\\s)+");

    public static String clean(String in) {
        /*
         * - remove punctuation
         * 
         * - normalize case
         * 
         * - remove extra spaces
         * 
         * - repalce space with FuzzyJoinDriver.TOKEN_SEPARATOR
         */

        in = rePunctuation.matcher(in).replaceAll(" ");
        in = reSpaceOrAnderscore.matcher(in).replaceAll(" ");
        in = in.trim();
        //in = in.replace(' ', '_');
        in = in.toLowerCase();
        return in;
    }
	public static List ArrayToList(String[] str){
		return Arrays.asList(str);
	}
}
