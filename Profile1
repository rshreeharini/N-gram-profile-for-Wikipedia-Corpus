import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class Profile1 {

    public static class Mapper1 extends Mapper<Object, Text, Text, NullWritable> {
        //private final static IntWritable one = new IntWritable();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {
                StringTokenizer token = new StringTokenizer(value.toString().split("<====>")[2]);
                while (token.hasMoreTokens()) {
                    String out= token.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                    if(out!=" ")
                    {
                        word.set(out);
                        context.write(word,NullWritable.get());
                    }
                }
            }
        }
    }
    public static class Reducer1 extends Reducer<Text, NullWritable, Text, NullWritable> {
        int count=0;
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            while (count <= 500) {
                context.write(key,NullWritable.get());
                count++;
            }

            }
        }
        public   static   void   main(String[]   args)   throws   Exception {
        Configuration conf = new Configuration();
        Job job=Job.getInstance(conf, "profile");
        job.setJarByClass(Profile1.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}







