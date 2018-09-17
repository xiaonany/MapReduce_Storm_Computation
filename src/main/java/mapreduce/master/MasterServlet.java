import java.io.*;  
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import javax.servlet.http.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import stormlite.bolt.PrintBolt;
import stormlite.spout.MyFileSpout;
import stormlite.Config;
import stormlite.Topology;
import stormlite.TopologyBuilder;
import stormlite.bolt.MapBolt;
import stormlite.bolt.ReduceBolt;
import stormlite.distributed.WorkerJob;
import stormlite.spout.FileSpout;
import stormlite.tuple.Fields;

public class MasterServlet extends HttpServlet {
  private static final String FILE_SPOUT = "FILE_SPOUT";
  private static final String MAP_BOLT = "MAP_BOLT";
  private static final String REDUCE_BOLT = "REDUCE_BOLT";
  private static final String PRINT_BOLT = "PRINT_BOLT";
  static final long serialVersionUID = 455555001;
  
  //set up two hashMap to store the workerStatus and the corresponding lastRecieved time of them
  private HashMap<String,workerStatus> workers = new HashMap<>();
  private HashMap<String,Date> lastRecievedTime = new HashMap<>();
  
  //set up an inner class workerStatus including all the worker info in it
  public class workerStatus{
	  String port;
	  String status;
	  String job;
	  String keysRead;
	  String keysWrittern;
	  public workerStatus(String port, String status, String job, String keysRead, String keysWrittern){
		  this.port = port;
		  this.status = status;
		  this.job = job;
		  this.keysRead = keysRead;
		  this.keysWrittern = keysWrittern;
	  }
	  public String getStatus() {return this.status;}
	  public String getJob(){return this.job;}
	  public String getKeysRead() {return this.keysRead;}
	  public String getKeysWrittern() {return this.keysWrittern;}
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
	  String URI = request.getRequestURI();
	  //handle /workerStatus get request
	  if (URI.equals("/workerstatus")){ 
		  //get the worker info from the request
		  String port = request.getParameter("port");
		  String status = request.getParameter("status");
		  String job = request.getParameter("job");
		  String keysRead = request.getParameter("keysRead");
		  String keysWrittern = request.getParameter("keysWrittern");
		  //get IP address of the remote worker
		  String addr = request.getRemoteAddr();
		  //wrap the info above up and store it in the workerStatus hashMap
		  workerStatus Status = new workerStatus(port, status, job, keysRead, keysWrittern);
		  workers.put(addr+":"+port, Status);
		  //store the recieving time of this info
		  Date curt= new Date();
		  lastRecievedTime.put(addr+":"+port, curt);
		  
	  //handle /status get request
	  }else if (URI.equals("/status")){
		  //print all the worker info first as a table
		  response.setContentType("text/html");
		  PrintWriter out = response.getWriter();
		  out.println("<html><head><title>Master</title></head><body><h1>XIAONAN YANG</h1><b/><h1>SEAS LOGIN:xiaonany</h1><br/><h3>Worker Status:</h3>");
		  out.println("<table border=\"1\" align=\"left\"><th>IP:port</th><th>status</th><th>job</th><th>keysRead</th><th>keysWrittern</th>");
		  for (String Addr:workers.keySet()){
			  if (new Date().getTime() - lastRecievedTime.get(Addr).getTime() < 30000){
				  workerStatus workerStatus = workers.get(Addr);
				  String content = "<tr><td>"+Addr+"</td><td>"+workerStatus.getStatus();
				  content += "</td><td>"+workerStatus.getJob();
				  content += "</td><td>"+workerStatus.getKeysRead();
				  content += "</td><td>"+workerStatus.getKeysWrittern()+"</tr>";
				  out.println(content);
			  }else{
				  workers.remove(Addr);
				  lastRecievedTime.remove(Addr);
			  }
		  }
		  out.println("</table><br/>");
		  
		  //then print a form for submitting jobs, with the action /submitJob as a Post request
		  out.println("</br></br><br/></br><br/></br><br/><h3>Job Submission:</h3>");
		  out.println("<form action = \"/submitJob\" method = \"post\">");
		  out.println("className: <input type=\"text\" name=\"className\"><br/>");
		  out.println("inputDir: <input type=\"text\" name=\"inputDir\"><br/>");
		  out.println("OutputDir: <input type=\"text\" name=\"OutputDir\"><br/>");
		  out.println("MapThreadsNum: <input type=\"text\" name=\"MapThreadsNum\"><br/>");
		  out.println("ReduceThreadsNum: <input type=\"text\" name=\"ReduceThreadsNum\"><br/>");
		  out.println("<input type=\"submit\" value = \"Submit\"></form>");
		  out.flush();
		  out.close();
		 
	  //handle /shutdown get request
	  }else if (URI.equals("/shutdown")){
		  int num = workers.keySet().size();
		  //for all the workers in the workerList, send shutdown request to trigger them shut down
		  for (String worker : workers.keySet()){
			  //open a socket to the worker
			  String host = worker.split(":")[0];
			  int port = Integer.parseInt(worker.split(":")[1]);
			  Socket socket = new Socket(host,port);
			  //send shutdown request to them
			  BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
			  PrintWriter out = new PrintWriter(writer);
			  out.write("POST /shutdown HTTP/1.1\r\n");
			  out.write("HOST: "+host+"\r\n");
			  out.write("User-Agent: cis455Master\r\n\r\n");
			  out.flush();
			  socket.close();
		  }
		  //after all workers shut down, clear the worker list in case of send shutDown request to any worker which is already shut down
		  workers.clear();
		  PrintWriter out = response.getWriter();
		  out.println("<html><head><title>shutdown</title></head><body><h2>" + num+
		  		" workers has been succssfully shot down!</h2></body></html>");
		
	  //no any URI as above, display a welcome page of the master	  
	  }else{
		  response.setContentType("text/html");
		  PrintWriter out = response.getWriter();
		  out.println("<html><head><title>Master</title></head>");
		  out.println("<body>Hi, I am the master!</body></html>");
	  }
  }
  
  public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
	  String URI = request.getRequestURI();
	  //handle the submitJob post request
	  if (URI.equals("/submitJob")){
		  //get all the job info
		  String className = request.getParameter("className");
		  String inputDir = request.getParameter("inputDir");
		  String OutputDir = request.getParameter("OutputDir");
		  String MapThreadsNum = request.getParameter("MapThreadsNum");
		  String ReduceThreadsNum = request.getParameter("ReduceThreadsNum");
		  //set up the job config object
		  Config config = new Config();
		  StringBuilder workerList = new StringBuilder();
		  workerList.append("[");
		  for (String Addr:workers.keySet()){
			  workerList.append(Addr+",");
		  }
		  String workerListStr = workerList.toString().substring(0,workerList.toString().length()-1)+"]";	  
		  config.put("workerList", workerListStr);		  
		  config.put("jobName", className);
	      config.put("mapClass", className);
	      config.put("reduceClass", className); 	      
		  config.put("inputDir", inputDir);
		  config.put("OutputDir", OutputDir);		  
	      config.put("spoutExecutors", "1");
	      config.put("mapExecutors", MapThreadsNum);
	      config.put("reduceExecutors", ReduceThreadsNum);
	      
	      //set up the topology object as well as the job object
		  Topology topo = createTopology(config);
		  WorkerJob job = new WorkerJob(topo, config);
			
		  ObjectMapper mapper = new ObjectMapper();
	      mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
	      try {
	    	  //send define job request to all workers
	    	  int i = 0;
	    	  for (String dest: workers.keySet()) {
			    config.put("workerIndex", String.valueOf(i++));
			    if (sendJob(dest, "POST", config, "definejob", 
			    		mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job)).getResponseCode() != 
			    		HttpURLConnection.HTTP_OK) {
			    	throw new RuntimeException("Job definition request failed");
				}
	    	  }
	    	  //send run job request to all workers
	    	  for (String dest: workers.keySet()) {
	    		  if (sendJob(dest, "POST", config, "runjob", "").getResponseCode() != 
	    				  HttpURLConnection.HTTP_OK) {
	    			  throw new RuntimeException("Job execution request failed");
	    		  }
	    	  }
	      } catch (JsonProcessingException e) {
	    	  System.exit(0);
	      }
	      //print the job submission success info and details
	      response.setContentType("text/html");
	      PrintWriter out = response.getWriter();
	      out.println("<html><head><title>Creating Job</title></head><body>");
	      out.println("<h3>Successfully create the job with the follwing parameters:</h3>");
	      out.println("job className:"+className+"<br>"+
	    		  	  "inputDir:"+inputDir+"<br>"+
	    		  	  "OutputDir:"+OutputDir+"<br>"+	
	    		  	  "MapThreadsNum:"+MapThreadsNum+"<br>"+
	    		  	  "ReduceThreadsNum:"+ReduceThreadsNum+"</body></html>");
	  }
  }
  
  //send json object to the given destination
  static HttpURLConnection sendJob(String dest, String reqType, Config config, String job, String parameters) throws IOException {
		URL url = new URL("http://" + dest + "/" + job);
//		log.info("Sending request to " + url.toString());
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod(reqType);
		if (reqType.equals("POST")) {
			conn.setRequestProperty("Content-Type", "application/json");	
			OutputStream os = conn.getOutputStream();
			byte[] toSend = parameters.getBytes();
			os.write(toSend);
			os.flush();
		} else
			conn.getOutputStream();	
		return conn;
  }
  
  //set up the topology with the config object
  public Topology createTopology(Config config){
	  FileSpout spout = new MyFileSpout();
      MapBolt mapBolt = new MapBolt();
      ReduceBolt reduceBolt = new ReduceBolt();
      PrintBolt printer = new PrintBolt();
	  TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout(FILE_SPOUT, spout, 1);
      builder.setBolt(MAP_BOLT, mapBolt, Integer.valueOf(config.get("mapExecutors"))).fieldsGrouping(FILE_SPOUT, new Fields("value"));
      builder.setBolt(REDUCE_BOLT, reduceBolt, Integer.valueOf(config.get("reduceExecutors"))).fieldsGrouping(MAP_BOLT, new Fields("key"));
      builder.setBolt(PRINT_BOLT, printer, 1).firstGrouping(REDUCE_BOLT);
      Topology topo = builder.createTopology();
      return topo;
  }
}
  
