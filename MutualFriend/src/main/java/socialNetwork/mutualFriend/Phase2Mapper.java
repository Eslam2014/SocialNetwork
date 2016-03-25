package socialNetwork.mutualFriend;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

/**
 * Phase1Mapper: emits one-degree and 2nd degree connections
 *
 * @author Mahmoud Parsian
 */
public class Phase2Mapper
    extends Mapper<LongWritable, Text, PairOfLongs, LongWritable> {

    final static LongWritable ZERO = new LongWritable(0);
    final static LongWritable ONE = new LongWritable(1);
    
    /**
     * Build a PairOfLongs(x,y) where x <= y
     */
    private static PairOfLongs buildSortedPairOfLongs(long a, long b) {
       if (a <= b) {
           return new PairOfLongs(a, b);
       }
       else {
           return new PairOfLongs(b, a);
       }
    }
    
    /**
     * @param key MR generated, ignored here
     * @param value has this format: <person><:><friend1><,><friend2><,>...
     */
    public void map(LongWritable key, Text value, Context context) 
       throws IOException, InterruptedException {
       String valueAsString = value.toString().replaceAll("\\s+", "");
       
      // valueAsString.replaceAll("\\s+", "");
       System.out.println(valueAsString);
       if ((valueAsString == null) || (valueAsString.length() == 0)) {
          return;
       }
       
       String[] tokens = StringUtils.split(valueAsString, ":");
       long person = Long.parseLong(tokens[0]);
       String friendsAsCSV = tokens[1];
       String[] friendsAsArray = StringUtils.split(friendsAsCSV, ",");
       
       // add all friends to a list    
       List<Long> friends =  new ArrayList<Long>();
       for (String friendAsString : friendsAsArray) {
          long friend = Long.parseLong(friendAsString);
          friends.add(friend);
       }

       // sort friends IDs
       Collections.sort(friends);
       for (int i = 0; i < friends.size(); i++) {
          long f1 = friends.get(i);

          // create a key representing the user and direct friend
          // assure that the lower of user and f1 is first in the key
          // this is a direct (first degree) connection, therefore we
          // flag this connection by using the zero flag
          PairOfLongs s1 = buildSortedPairOfLongs(person, f1);
          context.write(s1, ZERO);

          for (int j = i+1; j < friends.size(); j++) {
             long f2 = friends.get(j);
             // (f1, f2) represents 2nd-degree of connection
             PairOfLongs s2 = new PairOfLongs(f1, f2);

             // f1 is always <= f2 because we have sorted the friends
             // the s2 connection represents one 2nd-degree of connection, 
             // therefore, we use a ONE to represent one connection
             context.write(s2, ONE);
             
          }
       }
   }
}

