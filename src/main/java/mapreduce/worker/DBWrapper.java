import java.io.File; 
import java.util.ArrayList;
import java.util.HashMap;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

public class DBWrapper {

	private Environment Envir;
	private EnvironmentConfig EnvirConfig;
	//set a hash map to store different stores of different reducers, the hash key is the executorId of the reducer
	private HashMap<String,EntityStore> databases = new HashMap<>();
	
	public DBWrapper(String EnvirDir){
		//setup of the database environment of any worker
		EnvirConfig = new EnvironmentConfig();
		EnvirConfig.setTransactional(true);
		EnvirConfig.setAllowCreate(true);
		//clear database each time running a job
		File file = new File(EnvirDir+"/");
    	File[] files = file.listFiles();    
    	for (File f:files){
    		if(file.delete())
    			System.out.println("delete:"+f.getAbsolutePath());
    	}
    	file.mkdirs();
		Envir = new Environment(file, EnvirConfig);
	}
	
	//create a new store to store the key value pairs of a given reducer
	public void addDB(String executorId){
		StoreConfig storeConfig = new StoreConfig();
		storeConfig.setAllowCreate(true);
		storeConfig.setTransactional(true);
		EntityStore store = new EntityStore(Envir, executorId, storeConfig);
		//record the store with the key, the executorId of the corresponding store
		databases.put(executorId, store);
	}
	
	//add the key value pair to the store corresponding to the given executorId
	public void addValue(String executorId, String key, String value){
		//get the corresponding store from the hashMap
		EntityStore store = databases.get(executorId);
		//put the key value pair into the store
		PrimaryIndex<String,keyValue> keyValuePairs = store.getPrimaryIndex(String.class, keyValue.class);
		ArrayList<String> lst = new ArrayList<>();
		if (!keyValuePairs.contains(key+" "+value)){
			keyValue pair = new keyValue(key+" "+value,1);
			keyValuePairs.put(pair);
		}
		else{
			keyValue pair = keyValuePairs.get(key);
			pair.add();
			keyValuePairs.put(pair);
		}
	}
	
	//return all the key value pairs in the store which corresponding to the given executorId
	public HashMap<String, Integer> getAllPairs(String executorId){
		EntityStore store = databases.get(executorId);
		PrimaryIndex<String,keyValue> keyValuePairs = store.getPrimaryIndex(String.class, keyValue.class);
		HashMap<String, Integer> res = new HashMap<>();
		EntityCursor<keyValue> cursor = keyValuePairs.entities();
		for (keyValue kv:cursor){
			res.put(kv.getKey(), kv.getCount());
		}
		cursor.close();
		return res;
	}
}
