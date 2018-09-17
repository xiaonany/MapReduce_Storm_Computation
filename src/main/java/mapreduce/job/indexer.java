import mapreduce.Context;
import mapreduce.Job;

public class indexer implements Job{

	@Override
	public void map(String key, String value, Context context) {
		context.write(key,value);
	}

	@Override
	public void reduce(String key, Integer count, Context context, int totalCount) {
		try{
	    	float freq = (float) count/(float)totalCount;
	    	context.write(key, Float.toString(freq));
	    } catch (NumberFormatException e){
	    	
	    }
	}
}