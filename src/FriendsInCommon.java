import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FriendsInCommon extends Configured implements Tool {

	private static boolean rightOrder(String one, String two) {
			for(int i = 0; i < Math.min(one.length(), two.length()); i++) {
				char onechar = one.charAt(i);
				char twochar = two.charAt(i);
				if(onechar > twochar)
					return false;
				else if(twochar > onechar)
					return true;
				}
			return (one.length() > two.length()) ? false : true;
		}
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Text person = new Text();
        private Text friends = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            
            String[] split = line.split("->");
            person.set(split[0]);
            
            split = split[1].split("\\s");
            for(int i = 0; i < split.length; i++) {
            	for(int ii = i + 1; ii < split.length; ii++) {
            		if(FriendsInCommon.rightOrder(split[i], split[ii]))
            			friends = new Text(split[i] + "+" + split[ii]);
            		else
            			friends = new Text(split[ii] + "+" + split[i]);
                    context.write(friends, person);
            	}
            }
            //context.write(this.person, this.friends);
            }
        }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	String friends = "";
        	int num = 0;
            for (Text val : values) {
                friends += val + " ";
                num++;
            }
            friends += " | " + Integer.valueOf(num).toString();
            context.write(key, new Text(friends));
        }
    }

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(FriendsInCommon.class);
        job.setJobName("FriendsInCommon");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setNumReduceTasks(1);

        // Note that these are the default.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new FriendsInCommon(), args);
        System.exit(res);	
    }
}
