import static spark.Spark.setPort; 

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.log4j.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import stormlite.DistributedCluster;
import stormlite.TopologyContext;
import stormlite.distributed.WorkerJob;
import stormlite.routers.StreamRouter;
import stormlite.tuple.Tuple;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

/**
 * Simple listener for worker creation 
 * 
 * @author zives
 *
 */
public class WorkerServer {
    static Logger log = Logger.getLogger(WorkerServer.class);       
    public static DistributedCluster cluster = new DistributedCluster();
    static List<String> topologies = new ArrayList<>();
//    List<TopologyContext> contexts = new ArrayList<>();
    int myPort;  
    //the worker store directory
    public static String dir;
    //the worker database wrapper
    public static DBWrapper db;
    //the address of the master, used for sending /workerstatus request to
    String masterAddr;
    //assuming there is only one job per worker, this is the topologyContext of the job in this worker
    TopologyContext context = null; 
    //the current job of this worker
    String job = null; 

    //constructor of workerServer class with three input arguments from command line
    public WorkerServer(final String masterAddr, final String Nodedir, final int myPort) throws MalformedURLException {
    	//initialize all the fields
    	this.masterAddr = masterAddr;
    	this.myPort = myPort;
    	dir = Nodedir;
    	db = new DBWrapper(dir);
    	
    	//As soon as the worker is launched, set up an thread that send workerStatus periodically to master
    	TimerTask timerTask = new TimerTask() {
			@Override
			public void run() {
				try{
					//prepare the reporting URL with proper parameters, which is derived from the topologyContext of the job(which is also of the worker)
					String URLString = "http://" + masterAddr + "/workerstatus";
					URLString += "?port=" + myPort + "&status=" + getWorkerStatus() + "&job=" + getJob() 
							+ "&keysRead=" + getKeysRead() + "&keysWrittern=" + getKeysWrittern();
					//use URLEncoder to encode the results list
					URLString += "&results=" + URLEncoder.encode(getResults(),"UTF-8");

					//make conncetion to the master get send the URL out
					URL url = new URL(URLString);				
					HttpURLConnection conn = (HttpURLConnection) url.openConnection();
					conn.setDoOutput(true);
					conn.setRequestMethod("GET");
					conn.getResponseCode();
				}catch (Exception e){
					e.printStackTrace();
				}
			}
		};
    	//set the period of the timerTask
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(timerTask, 30, 10000);
		
        log.info("Creating server listener at socket " + myPort);        
        setPort(myPort);
        final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        //listen to the server port of the worker and handle defineJob request 
        Spark.post("/definejob", new Route() {
                @Override
                public Object handle(Request arg0, Response arg1) {                      
                    WorkerJob workerJob;
                    try {
                        workerJob = om.readValue(arg0.body(), WorkerJob.class);
                                
                        try {
                            log.info("Processing job definition request" + workerJob.getConfig().get("jobName") +
                                     " on machine " + workerJob.getConfig().get("workerIndex"));
                            //get the job name, which is the class name
                            job = workerJob.getConfig().get("jobName");
                            //set up the topologyContext
                            context = cluster.submitTopology(job, workerJob.getConfig(), workerJob.getTopology(), dir);                        
//                            contexts.add(context);
                            synchronized (topologies) {
                                topologies.add(workerJob.getConfig().get("jobName"));
                            }
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                        return "Job launched";
                    } catch (IOException e) {
                        e.printStackTrace();                                       
                        // Internal server error
                        arg1.status(500);
                        return e.getMessage();
                    }         
                }              
            });
        
        //handle runJob request
        Spark.post("/runjob", new Route() {
                @Override
                public Object handle(Request arg0, Response arg1) {
                    log.info("Starting job!");
                    cluster.startTopology();
                                
                    return "Started";
                }
            });
        
        //handle push stream request
        Spark.post("/push/:stream", new Route() {
                @Override
                public Object handle(Request arg0, Response arg1) {
                    try {
                        String stream = arg0.params(":stream");
                        Tuple tuple = om.readValue(arg0.body(), Tuple.class);                                      
                        log.debug("Worker received: " + tuple + " for " + stream);                                        
                        // Find the destination stream and route to it
                        StreamRouter router = cluster.getStreamRouter(stream);                        
//                        if (contexts.isEmpty())
//                            log.error("No topology context -- were we initialized??");                                        
                        if (!tuple.isEndOfStream())
                            context.incSendOutputs(router.getKey(tuple.getValues()));                                        
                        if (tuple.isEndOfStream())
                            router.executeEndOfStreamLocally(context);
                        else
                            router.executeLocally(tuple, context);              
                        return "OK";
                    } catch (IOException e) {
                        e.printStackTrace();
                                        
                        arg1.status(500);
                        return e.getMessage();
                    }
                                
                }
                
            });
        
      //handle shutdown request
        Spark.post("/shutdown", new Route(){
        	@Override
        	public Object handle(Request arg0, Response arg1){
        		//call the shutdown method to shutdown the worker
        		shutdown();
        		return "OK";
        	}       	
        });
    }
        
    public static void createWorker(String masterAddr, String dir, int port) throws MalformedURLException {
//        if (!config.containsKey("workerList"))
//            throw new RuntimeException("Worker spout doesn't have list of worker IP addresses/ports");
//
//        if (!config.containsKey("workerIndex"))
//            throw new RuntimeException("Worker spout doesn't know its worker ID");
//        else {
//            String[] addresses = WorkerHelper.getWorkers(config);
//            String myAddress = addresses[Integer.valueOf(config.get("workerIndex"))];
//
//            log.debug("Initializing worker " + myAddress);
//
//            URL url;
//            try {
//                url = new URL(myAddress);
//
//                new WorkerServer(masterAddr, dir, url.getPort());
//            } catch (MalformedURLException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
//        }
    	
    	//contruct an new worker with the given parameters form the command line
    	new WorkerServer(masterAddr, dir, port);
    }

    public static void shutdown() {
        synchronized(topologies) {
            for (String topo: topologies)
                cluster.killTopology(topo);
        }      
        cluster.shutdown();
        //get the worker application exit
        System.exit(0);
    }

    //get keysRead from the topologyContext
    public int getKeysRead(){
    	if (context == null)
    		return 0;
    	return context.getKeysRead();
    }
    
    //get keysWrittern from the topologyContext
    public int getKeysWrittern(){
    	if (context == null)
    		return 0;
    	return context.getKeysWrittern();
    }
    
  //get results from the topologyContext
    public String getResults(){
    	if (context == null){
    		return "NoResults";
    	}
    	return context.getResults().toString();
    }
    
  //get job name considering whether any job exists
    public String getJob(){
    	if (job == null){
    		return "NoJob";
    	}
    	return job;
    }
    
  //get workerStatus from the topologyContext
    public String getWorkerStatus(){
    	if (context == null)
    		return "idle";
    	if (context.getState() == TopologyContext.STATE.IDLE)
    		return "idle";
    	else if (context.getState() == TopologyContext.STATE.WAITING)
    		return "waiting";
    	else if (context.getState() == TopologyContext.STATE.REDUCE)
    		return "reducing";
    	else if (context.getState() == TopologyContext.STATE.MAP)
    		return "mapping";
    	else
    		return null;
    }
}
