package socialNetwork.mutualFriend;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author Eslam Ali
 *
 */
public class MutualFriendsRecommederDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: numberOfRecommendations <input-path> <output-path>");
			System.exit(-1);
		}
		int status = ToolRunner.run(new MutualFriendsRecommederDriver(), args);
		System.exit(status);

	}

	public int run(String[] args) throws Exception {
		String inputPath = args[1];
		String outputPath = args[2];
		int numberOfRecommendations = Integer.parseInt(args[0]);

		// phase 1
		Job job1 = new Job(getConf(), "job1");
		job1.setJarByClass(MutualFriendsRecommederDriver.class);

		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(LongWritable.class);

		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(Text.class);

		job1.setMapperClass(Phase1Mapper.class);
		job1.setReducerClass(Phase1Reducer.class);
		// job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path("phase1"));
		job1.waitForCompletion(true);

		// phase2:
		Job job2 = new Job(getConf(), "job2");
		job2.setJarByClass(MutualFriendsRecommederDriver.class);

		// mapper's output (K,V) classes
		job2.setMapOutputKeyClass(PairOfLongs.class);
		job2.setMapOutputValueClass(LongWritable.class);

		// reducer's output (K,V) classes
		job2.setOutputKeyClass(PairOfLongs.class);
		job2.setOutputValueClass(LongWritable.class);

		// identify map() and reduce() functions
		job2.setMapperClass(Phase2Mapper.class);
		job2.setReducerClass(Phase2Reducer.class);

		// identify format of I/O paths
		job2.setInputFormatClass(TextInputFormat.class);
		// create a SequenceFile, so we do not have to do further parsing
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job2, new Path("phase1"));
		FileOutputFormat.setOutputPath(job2, new Path("phase2"));
		job2.waitForCompletion(true);
		job2.waitForCompletion(true);

		// phase2:
		Job job3 = new Job(getConf(), "job3");
		job3.setJarByClass(MutualFriendsRecommederDriver.class);

		// the number of recommendations for a user to be generated
		job3.getConfiguration().setInt("number.of.recommendations", numberOfRecommendations);

		// mapper's output (K,V) classes
		job3.setMapOutputKeyClass(LongWritable.class);
		job3.setMapOutputValueClass(PairOfLongs.class);

		// reducer's output (K,V) classes
		job3.setOutputKeyClass(LongWritable.class);
		job3.setOutputValueClass(Text.class);

		// identify map() and reduce() functions
		job3.setMapperClass(Phase3Mapper.class);
		job3.setReducerClass(Phase3Reducer.class);

		// read from a SequenceFile, so we do not have to do further parsing
		job3.setInputFormatClass(SequenceFileInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job3, new Path("phase2"));
		FileOutputFormat.setOutputPath(job3, new Path(outputPath));

		boolean status3 = job3.waitForCompletion(true);
		return status3 ? 0 : 1;

	}

}
