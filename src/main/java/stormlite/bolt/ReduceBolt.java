import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.log4j.Logger;
import stormlite.OutputFieldsDeclarer;
import stormlite.TopologyContext;
import stormlite.distributed.WorkerHelper;
import stormlite.routers.StreamRouter;
import stormlite.tuple.Fields;
import stormlite.tuple.Tuple;
import mapreduce.Job;
import mapreduce.worker.WorkerServer;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "reduce"
 * on a per-tuple basis
 * 
 */
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

public class ReduceBolt implements IRichBolt {
	static Logger log = Logger.getLogger(ReduceBolt.class);

	
	Job reduceJob;

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
	Fields schema = new Fields("key", "value");
	
	boolean sentEof = false;
	
	/**
	 * Buffer for state, by key
	 */
//	Map<String, List<String>> stateByKey = new HashMap<>();

	/**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    
    private TopologyContext context;
    
    int neededVotesToComplete = 0;
    
    public ReduceBolt() {
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;
        
        //for each reducer, create a store for it and record its store with its executorId
        WorkerServer.db.addDB(executorId);
        
        if (!stormConf.containsKey("reduceClass"))
        	throw new RuntimeException("Mapper class is not specified as a config option");
        else {
        	String mapperClass = stormConf.get("reduceClass");
        	
        	try {
				reduceJob = (Job)Class.forName(mapperClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + mapperClass);
			}
        }
        if (!stormConf.containsKey("mapExecutors")) {
        	throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
        }

        //determine how many EOS votes needed
        int workersNum = WorkerHelper.getWorkers(stormConf).length;
        int mapExecutorsNum = Integer.parseInt(stormConf.get("mapExecutors"));
        int reduceExecutorsNum = Integer.parseInt(stormConf.get("reduceExecutors"));
        neededVotesToComplete = (workersNum-1)*mapExecutorsNum*reduceExecutorsNum + mapExecutorsNum;
    }

    /**
     * Process a tuple received from the stream, buffering by key
     * until we hit end of stream
     */
    @Override
    public synchronized void execute(Tuple input) {
    	if (sentEof) {
	        if (!input.isEndOfStream())
	        	throw new RuntimeException("We received data after we thought the stream had ended!");
    		// Already done!
		} else if (input.isEndOfStream()) {
			// only if the EOS we needed equals 0 do we trigger the reduce operation and output all state
			neededVotesToComplete -= 1;
			if (neededVotesToComplete == 0){
				//get all the key value pairs in the store of this reducer
				HashMap<String, Integer> pairs = WorkerServer.db.getAllPairs(executorId);
				//change the worker status to reduce
				if (context.getState() == TopologyContext.STATE.WAITING){
					context.setState(TopologyContext.STATE.REDUCE);
					context.setKeysRead(0);
					context.setKeysWrittern(0);
				}
				//start the reducing job and change the keys read and written so far
				for (String key:pairs.keySet()){
					int totalNum = collector.context.getCount(key.split(" ")[1]);
					context.keysReadIncre();
					reduceJob.reduce(key, pairs.get(key), collector,totalNum);
					context.keysWritternIncre();
				}
				//send EOS to the printBolt
				collector.emitEndOfStream();
				sentEof = true;
			}
			
    	} else {
    		//store the key value pairs in the local BDB
    		String key = input.getStringByField("key");
	        String value = input.getStringByField("value");
	        log.debug(getExecutorId() + " received " + key + " / " + value);        
	        WorkerServer.db.addValue(executorId,key,value);
    	}        
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
    }

    /**
     * Lets the downstream operators know our schema
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    /**
     * Used for debug purposes, shows our exeuctor/operator's unique ID
     */
	@Override
	public String getExecutorId() {
		return executorId;
	}

	/**
	 * Called during topology setup, sets the router to the next
	 * bolt
	 */
	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}
}
