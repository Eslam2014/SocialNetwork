package socialNetwork.mutualFriend;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hsqldb.lib.StringUtil;

/**
 * Phase1Mapper emit friends of user
 * @author Eslam Ali
 *
 */
public class Phase1Mapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
	
	/**
	 * @param Key MP generated , ignored here .
	 * @param value has this format (person,friend)
	 */
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
	    if(value==null || value.equals(" ")){
	    	return;
	    }
	    String token[]=StringUtil.split(value.toString(), ",");
	    context.write(new LongWritable(Long.parseLong(token[0])), new LongWritable(Long.parseLong(token[1])));	
	}

}
