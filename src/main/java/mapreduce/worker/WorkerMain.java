import java.net.MalformedURLException; 

public class WorkerMain {
    public static void main(String[] args) throws MalformedURLException {
    	//if there are less than 3 arguments in the command line, print my name and seas login id and exit
    	if (args.length < 3){
    		System.out.println("XIAONAN YANG, SEAS Login: xiaonany");
    		System.exit(0);
    	}else{
    		//read in the three arguments and create the specific worker
    		String masterAddr = args[0];
    		String storeDir = args[1];
    		int port = Integer.parseInt(args[2]);
            WorkerServer.createWorker(masterAddr, storeDir, port);
    	}
    	
    }
}
