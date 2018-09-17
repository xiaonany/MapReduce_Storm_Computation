import java.util.Map; 
import java.util.UUID;

import org.apache.log4j.Logger;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;

import stormlite.OutputFieldsDeclarer;
import stormlite.TopologyContext;
import stormlite.distributed.WorkerHelper;
import stormlite.routers.StreamRouter;
import stormlite.tuple.Fields;
import stormlite.tuple.Tuple;
import mapreduce.Context;
import mapreduce.Job;
import mapreduce.master.MasterServlet.workerStatus;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "map" 
 * on a per-tuple basis.
 * 
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

public class MapBolt implements IRichBolt {
	static Logger log = Logger.getLogger(MapBolt.class);

	Job mapJob;

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
	Fields schema = new Fields("key", "value");
	
	/**
	 * This tracks how many "end of stream" messages we've seen
	 */
	int neededVotesToComplete = 0;

	/**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    
    private TopologyContext Context;
    
    public MapBolt() {
    	
    }
    
	/**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.Context = context;
        
        
        if (!stormConf.containsKey("mapClass"))
        	throw new RuntimeException("Mapper class is not specified as a config option");
        else {
        	String mapperClass = stormConf.get("mapClass");
        	
        	try {
				mapJob = (Job)Class.forName(mapperClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + mapperClass);
			}
        }
        
        if (!stormConf.containsKey("spoutExecutors")) {
        	throw new RuntimeException("Mapper class doesn't know how many input spout executors");
        }
        
        // determine how many end-of-stream requests are needed from the config of the job
        int NumOfSpout = Integer.parseInt(stormConf.get("spoutExecutors"));
        int NumOfMapper = Integer.parseInt(stormConf.get("mapExecutors"));
        neededVotesToComplete = NumOfSpout*(WorkerHelper.getWorkers(stormConf).length-1)*NumOfMapper + NumOfSpout;
    }

    /**
     * Process a tuple received from the stream, incrementing our
     * counter and outputting a result
     */
    @Override
    public synchronized void execute(Tuple input) {
    	if (!input.isEndOfStream()) {
	        String key = input.getStringByField("key");
	        String value = input.getStringByField("value");
	        log.debug(getExecutorId() + " received " + key + " / " + value);
	        
	        if (neededVotesToComplete == 0)
	        	throw new RuntimeException("We received data after we thought the stream had ended!");
	        
	        //call the mapper, and do bookkeeping to track work done
//	        Context.keysReadIncre();
	        mapJob.map(key, value, collector);
//	        Context.keysWritternIncre();
	        Context.increCount(key);
	        Context.setState(TopologyContext.STATE.MAP);
	        
    	} else if (input.isEndOfStream()) {
    		//determine what to do with EOS
    		neededVotesToComplete -= 1;
    		if (neededVotesToComplete == 0){
    			collector.emitEndOfStream();
    			Context.setState(TopologyContext.STATE.WAITING);
    		}
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
