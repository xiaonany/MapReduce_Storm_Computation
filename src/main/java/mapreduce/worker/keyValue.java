import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

//key value pair entity
@Entity
public class keyValue{
	//with key as the primary key
	@PrimaryKey
	private String key;
	private int count;
//	private ArrayList<String> valueList;
	public keyValue() {}
	public keyValue(String key,int count){
		this.key = key;
		this.count = count;
	}
	
	//add a value to the valueList
	public void add(){ 
		this.count++;
	} 

	public String getKey(){ 
		return this.key; 
	}
	
	public int getCount() { 
		return this.count; 
	} 
}
