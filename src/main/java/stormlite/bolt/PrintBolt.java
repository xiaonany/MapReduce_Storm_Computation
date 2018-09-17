import java.io.BufferedWriter; 
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.log4j.Logger;
import stormlite.OutputFieldsDeclarer;
import stormlite.TopologyContext;
import stormlite.bolt.IRichBolt;
import stormlite.bolt.OutputCollector;
import stormlite.distributed.WorkerHelper;
import stormlite.routers.StreamRouter;
import stormlite.tuple.Fields;
import stormlite.tuple.Tuple;
import mapreduce.worker.WorkerServer;

/**
 * A trivial bolt that simply outputs its input stream to the
 * console
 * 
 * @author zives
 *
 */
public class PrintBolt implements IRichBolt {
	static Logger log = Logger.getLogger(PrintBolt.class);
	
	Fields myFields = new Fields();

	int neededVotesToComplete = 0;
	
	TopologyContext context;
    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the PrintBolt, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    //have a writer to write output in the output directory
    BufferedWriter writer;
	@Override
	public void cleanup() {
	}
	
	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.context = context;
		//set up the writer with the output file
		String outputDir = WorkerServer.dir + "/" + stormConf.get("OutputDir") + "/output.txt";
		try {
			writer = new BufferedWriter(new FileWriter(outputDir));
		} catch (IOException e) {
			e.printStackTrace();
		}
		//compute the number of EOS needed to mark the data have been all written
		int reducerNum = Integer.parseInt(stormConf.get("reduceExecutors"));
		neededVotesToComplete = reducerNum*WorkerHelper.getWorkers(stormConf).length;
	}

	@Override
	public void execute(Tuple input) {
		if (!input.isEndOfStream()){
			synchronized (writer) {
				//write all key value pairs to the output file
				List<Object> output = input.getValues();
				try {
					writer.write(output.get(0) + "," + output.get(1) + "\n");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			//for every tuple, after writing it to output file, record them in topologyContext as well
			//for sending the results to master
			synchronized (context.getResults()) {
				if (context.getResults().size() <= 100)
					context.addToResults(input.toString());
			}
		//if not EOS
		}else{
			neededVotesToComplete --;
			//if EOS needed equals to 0, then the print process is accomplished
			if (neededVotesToComplete == 0){
				//set the worker status to idle
				context.setState(TopologyContext.STATE.IDLE);
				try{
					//shut down the writer
					writer.close();
					//shut down the topology cluster
					WorkerServer.cluster.shutdown();
				}catch (Exception e){
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(StreamRouter router) {
		// Do nothing
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}

}