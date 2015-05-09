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

package org.macau.join.phase2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.io.Writable;

public class ValueSelfJoin implements Writable {
	
    private int rid;
    
    //the set token and use the token rank number stands for the token
    private int[] tokens;
    //the position information
    private int x;
    private int y;

    public ValueSelfJoin() {
    
    }

    public ValueSelfJoin(int rid, int x, int y,Collection<Integer> tokens) {
        this.rid = rid;
        this.x = x;
        this.y = y;
        setTokens(tokens);
    }

    public ValueSelfJoin(ValueSelfJoin v) {
        rid = v.rid;
        x = v.x;
        y = v.y;
        tokens = v.tokens;
    }

    public int getRID() {
        return rid;
    }

    public int getX() {
		return x;
	}

	public void setX(int x) {
		this.x = x;
	}

	public int getY() {
		return y;
	}

	public void setY(int y) {
		this.y = y;
	}

	public int[] getTokens() {
        return tokens;
    }

    public void readFields(DataInput in) throws IOException {
        rid = in.readInt();
        x = in.readInt();
        y = in.readInt();
        tokens = new int[in.readInt()];
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = in.readInt();
        }
    }

    public void setRID(int rid) {
        this.rid = rid;
    }

    public void setTokens(Collection<Integer> tokens) {
        this.tokens = new int[tokens.size()];
        int i = 0;
        for (Integer token : tokens) {
            this.tokens[i++] = token;
        }
    }

    @Override
    public String toString() {
        return rid + ":" + Arrays.toString(tokens);
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(rid);
        out.writeInt(x);
        out.writeInt(y);
        out.writeInt(tokens.length);
        for (int token : tokens) {
            out.writeInt(token);
        }
    }
}
