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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GetPrime extends Configured implements Tool {
    public static class Map extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Integer number = new Integer(value.toString());
            if (number == 0)
                return;
            else if (number == 1)
                return;
            else if (number == 2) {
                context.write(new Text("qwert1"), new IntWritable(number));
                return;
            }
            for (int i = 2; i <= Math.sqrt(number); i++) {
                if (number % i == 0)
                    return;
            }
            context.write(new Text("qwert1"), new IntWritable(number));
        }
    }

    public static class CombinePrime extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            for (IntWritable val : values)
                context.write(new Text("prime"), val);
        }
    }

    public int run(String[] args) throws Exception {
        (FileSystem.get(getConf())).delete(new Path(args[1]), true);
        Job job = new Job(getConf());
        job.setJarByClass(GetPrime.class);
        job.setJobName("GetPrime");

        //getConf().setInt("mapred.line.input.format.linespermap", 10);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(CombinePrime.class);

        job.setNumReduceTasks(1);

        // Note that these are the default.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
/*
        Job multiplicator = new Job(getConf());
        multiplicator.setJarByClass(PrimeMultiplicator.class);
        multiplicator.setJobName("PrimeMultiplicator");
        multiplicator.setInputFormatClass(TextInputFormat.class);
        multiplicator.setOutputKeyClass(LongWritable.class);
        multiplicator.setOutputValueClass(Text.class);

        multiplicator.setMapperClass(PrimeMultiplicator.Map.class);
        multiplicator.setReducerClass(PrimeMultiplicator.Reduce.class);

        multiplicator.setNumReduceTasks(1);

        // Note that these are the default.
        multiplicator.setInputFormatClass(TextInputFormat.class);
        multiplicator.setOutputFormatClass(TextOutputFormat.class);
        
        multiplicator.getConfiguration().set("primemultiplicator.input.path", args[0]);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));*/
        
        
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(args[1]);
        int res = ToolRunner.run(new Configuration(), new GetPrime(), args);
        res = ToolRunner.run(new Configuration(), new PrimeMultiplicator(), args);
        System.exit(res);
    }
}