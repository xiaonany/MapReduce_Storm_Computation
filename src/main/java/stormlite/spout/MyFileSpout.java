
import stormlite.spout.FileSpout;
import mapreduce.worker.WorkerServer;

public class MyFileSpout extends FileSpout {
	//extends the fielSpout class and override the getFilename method
	@Override
	public String getFilename() {
		return WorkerServer.dir;
	}
}
