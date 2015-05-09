/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 * 
 * Author: Rares Vernica <rares (at) ics.uci.edu>
 */

package org.macau.paper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;

import org.macau.util.FuzzyJoinConfig;


public class TokenLoad implements Serializable {
    private final String path;
    private final TokenRank rank;

    public TokenLoad(String path, TokenRank rank) {
        this.path = path;
        this.rank = rank;
    }

    public void loadTokenRank() {
        loadTokenRank(1);
    }

    public void loadTokenRank(int factor) {
        try {
            BufferedReader fis = new BufferedReader(
            // new FileReader(path.toString())
                    new InputStreamReader(new FileInputStream(path), "UTF-8"));
            String token = null;
            while ((token = fis.readLine()) != null) {
            	System.out.println(token);
                rank.add(token);
                // only used when increasing the token dictionary
                for (int i = 1; i < factor; i++) {
                    // remove _COUNT at the end of the token (it is removed in
                    // the new records anyway)
                    rank.add(token.split(FuzzyJoinConfig.TOKEN_SEPARATOR_REGEX)[0]
                            + i);
                }
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    
    public static void main(String[] args){
    	System.out.println("goo");
    	TokenRank tokenRank = new TokenRankFrequency();
    	String path = "/home/hadoop/expected.txt";
    	TokenLoad tokenLoad = new TokenLoad(path, tokenRank);
        tokenLoad.loadTokenRank();
        
        System.out.println(tokenRank.toString());
    }
}