import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.annotation.Inherited;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

public class PrimeMultiplicator extends Configured implements Tool {
    
    public static class Map extends
            Mapper<LongWritable, Text, LongWritable, Text> {
        private FileSystem fs;
        String inputPath;
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Integer number = new Integer(value.toString().split("\\t")[1]);
            String line;
            DataInputStream d = new DataInputStream(fs.open(new Path(inputPath)));
            BufferedReader in = new BufferedReader(new InputStreamReader(d));
            while((line = in.readLine()) != null) {
                int val = Integer.valueOf(line.split("\\t")[1]);
                context.write(new LongWritable(val * number), new Text(val + " | " + number));
            }
            in.close();
            d.close();
        }
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            inputPath = context.getConfiguration().get("primemultiplicator.input.path");
            fs = FileSystem.get(context.getConfiguration());
            super.setup(context);
        }
    }

/*
    public static class Reduce extends
            Reducer<LongWritable, Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            Text value = new Text();
            for(Text val : values) {
                value = val;
            }
            context.write(key, value);
        }
    }*/

    public int run(String[] args) throws Exception {
        FileSystem sys = FileSystem.get(getConf());
        sys.delete(new Path(args[2]), true);
        Job job = new Job(getConf());
        job.setJarByClass(PrimeMultiplicator.class);
        job.setJobName("PrimeMultiplicator");
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        //job.setReducerClass(Reduce.class);

        job.setNumReduceTasks(0);
        // Note that these are the default.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.getConfiguration().set("primemultiplicator.input.path", args[1] + "/part-r-00000");

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileInputFormat.setMaxInputSplitSize(job, 1000);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("you cannot run this standalone, run GetPrime");
        //int res = ToolRunner.run(new Configuration(), new PrimeMultiplicator(), args);
        System.exit(100);
    }
}
