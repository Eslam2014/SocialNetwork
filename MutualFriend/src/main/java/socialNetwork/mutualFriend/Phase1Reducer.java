package socialNetwork.mutualFriend;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 *Phase1Reducer friends list of user
 * @author Eslam Ali
 *
 */
public class Phase1Reducer 
               extends Reducer<LongWritable, LongWritable, LongWritable, Text>{
	
	@Override
	protected void reduce(LongWritable userId, Iterable<LongWritable> frindsIds,
			Context context)
			throws IOException, InterruptedException {
		StringBuilder friends=new StringBuilder(":");
		
		Iterator<LongWritable> iterator=frindsIds.iterator();
		while (iterator.hasNext()) {
			friends.append(iterator.next());
			if(iterator.hasNext()){
				friends.append(",");
			}
			
		}
      context.write(userId, new Text(friends.toString()));
		
	}

}
