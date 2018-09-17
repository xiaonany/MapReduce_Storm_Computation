import java.util.Iterator; 
import mapreduce.Context;
import mapreduce.Job;
public class WordCount implements Job {

  public void map(String key, String value, Context context)
  {
	  context.write(value, ""+1);
  }
 
  public void reduce(String key, Iterator<String> values, Context context)
  {
    try{
    	if (key.equals("")||values==null)
    		return;
    	int count = 0;
    	while (values.hasNext()){
    		String v = values.next();
    		count += Integer.parseInt(v);
    	}
    	context.write(key, Integer.toString(count));
    } catch (NumberFormatException e){
    	
    }
  }
  
}
