import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortValue extends Configured implements Tool {
    public static class Map extends
            Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String val = value.toString();
            String[] split = val.split("\\t");
            context.write(new IntWritable(Integer.valueOf(split[1])), new Text(split[0]));
        }
    }

    public int run(String[] args) throws Exception {
        (FileSystem.get(getConf())).delete(new Path(args[1]), true);
        Job job = new Job(getConf());
        job.setJarByClass(SortValue.class);
        job.setJobName("SortValue");

        //getConf().setInt("mapred.line.input.format.linespermap", 10);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);

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
        int res = ToolRunner.run(new Configuration(), new SortValue(), args);
        System.exit(res);
    }
}