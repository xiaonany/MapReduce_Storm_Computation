/**
 * Licensed to the Apache Software Foundation (ASF) under one 
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import stormlite.routers.StreamRouter;

/**
 * Information about the execution of a topology, including
 * the stream routers
 * 
 * @author zives
 *
 */
public class TopologyContext {
	Topology topology;
	
	Queue<Runnable> taskQueue;
	
	public static enum STATE {WAITING, MAP, REDUCE, IDLE};
	
	STATE state = STATE.IDLE;
	
	Map<String, Integer> sendOutputs = new HashMap<>();
	
	/**
	 * Mappings from stream IDs to routers
	 */
	Map<String,StreamRouter> next = new HashMap<>();
	
	//create worker informations 
	int mapOutputs = 0;
	
	int reduceOutputs = 0;
	
	int keysRead = 0;
	
	int keysWrittern = 0;
	
	ArrayList<String> results;
	
	HashMap<String, Integer> wordsCount; 
	
	public TopologyContext(Topology topo, Queue<Runnable> theTaskQueue) {
		topology = topo;
		taskQueue = theTaskQueue;
		results = new ArrayList<>();
	}
	
	public Topology getTopology() {
		return topology;
	}
	
	public void setTopology(Topology topo) {
		this.topology = topo;
	}
	
	public void addStreamTask(Runnable next) {
		taskQueue.add(next);
	}

	public STATE getState() {
		return state;
	}

	public void setState(STATE state) {
		this.state = state;
	}

	public int getMapOutputs() {
		return mapOutputs;
	}

	public void incMapOutputs(String key) {
		this.mapOutputs++;
	}

	public int getReduceOutputs() {
		return reduceOutputs;
	}

	public void incReduceOutputs(String key) {
		this.reduceOutputs++;
	}
	
	public void incSendOutputs(String key) {
		 if (!sendOutputs.containsKey(key))
			 sendOutputs.put(key, new Integer(0));
		 
		 sendOutputs.put(key,  new Integer(sendOutputs.get(key) + 1));
	}
	
	public Map<String, Integer> getSendOutputs() {
		return sendOutputs;
	}
	
	//get/change keys read and written
	
	public void keysReadIncre(){
		this.keysRead += 1;
	}
	
	public void keysWritternIncre(){
		this.keysWrittern += 1;
	}
	
	public void setKeysRead(int i){
		this.keysRead = i;
	}
	
	public void setKeysWrittern(int i){
		this.keysWrittern = i;
	}
	
	public int getKeysRead(){
		return this.keysRead;
	}
	
	public int getKeysWrittern(){
		return this.keysWrittern;
	}
	
	//get/change results
	
	public ArrayList<String> getResults(){
		return this.results;
	}
	
	public void addToResults(String input){
		results.add(input);
	}
	
	public void increCount(String url){
		int count = wordsCount.get(url).intValue();
		wordsCount.put(url, count);
	}
	
	public int getCount(String url){
		return this.wordsCount.get(url);
	}
}
