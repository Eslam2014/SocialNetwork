package socialNetwork.mutualFriend;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A reducer class that just emits the sum of the input values for non-direct friends
 *
 *  @author Mahmoud Parsian
 */
public class Phase2Reducer 
    extends Reducer<PairOfLongs, LongWritable, PairOfLongs, LongWritable> {
   
    public void reduce(PairOfLongs key, Iterable<LongWritable> values, Context context) 
        throws IOException, InterruptedException {
       
        long numberOfMutualFriends = 0;
        for (LongWritable value : values) {
           // value of 0 indicates that the two users (user1=key.left, user2=key.right)
           // represented by this key are already direct friends, so no output will be emitted
           if (value.get() == 0) {
              return;
           }
        
           // otherwise tally the number of mutual friends
           numberOfMutualFriends += value.get();
        }

        // there is a possibility of friends recommendation
        // and numMutualFriends is the number of mutual friends
        context.write(key, new LongWritable(numberOfMutualFriends));
    }
}

