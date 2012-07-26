import java.io.BufferedReader;
import java.io.Console;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LSGAccessCount extends Configured implements Tool {

    public static class Map extends

    Mapper<LongWritable, Text, Text, IntWritable> {

        static enum Counters {

            INPUT_WORDS

        }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private boolean caseSensitive = true;
        private Set<String> patternsToSkip = new HashSet<String>();
        private long numRecords = 0;
        private String inputFile;

        public void setup(Context context) {

            Configuration conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
            inputFile = conf.get("mapreduce.map.input.file");

            if (conf.getBoolean("wordcount.skip.patterns", false)) {

                Path[] patternsFiles = new Path[0];
                
                try {
                    patternsFiles = DistributedCache.getLocalCacheFiles(conf);
                } catch (IOException ioe) {
                    System.err.println("Caught exception while getting cached files: "
                                       + StringUtils.stringifyException(ioe));
                }
                
                for (Path patternsFile : patternsFiles) {
                    parseSkipFile(patternsFile);
                }
            }
        }

        private void parseSkipFile(Path patternsFile) {

            try {
                BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
                String pattern = null;

                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                + patternsFile + "' : " + StringUtils.stringifyException(ioe));
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();

            for (String pattern : patternsToSkip) {
                line = line.replaceAll(pattern, "");
            }
            String[] split = line.split("\\s");
            split = split[6].split("\\?");
            word.set(split[0]);
            //word.set(line);
            context.write(word, one);
            context.getCounter(Counters.INPUT_WORDS).increment(1);
            
            if ((++numRecords % 100) == 0) {
                context.setStatus("Finished processing " + numRecords
                + " records " + "from the input file: " + inputFile);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        (FileSystem.get(getConf())).delete(new Path(args[1]), true);

        Job job = new Job(getConf());

        job.setJarByClass(LSGAccessCount.class);

        job.setJobName("LSGCount");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setNumReduceTasks(1);
        
        // Note that these are the default.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        List<String> other_args = new ArrayList<String>();

        for (int i = 0; i < args.length; ++i) {
            if ("-skip".equals(args[i])) {

                DistributedCache.addCacheFile(new Path(args[++i]).toUri(),

                job.getConfiguration());
                job.getConfiguration().setBoolean("wordcount.skip.patterns",
                true);
            } else {
                other_args.add(args[i]);
            }
        }

        FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LSGAccessCount(), args);
        String[] args2 = new String[2];
        args2[0] = args[1];
        args2[1] = args[2];
        res = ToolRunner.run(new Configuration(), new SortValue(), args2);
        System.exit(res);
    }

}