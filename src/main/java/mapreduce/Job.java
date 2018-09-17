public interface Job {

  void map(String key, String value, Context context);
  
  void reduce(String key, Integer count, Context context, int totalCount);
  
}
