import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;


public class UnitSum {

    public static class PassMap extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key,
                        Text value,
                        Context context) throws IOException, InterruptedException {
            // tag\tsubPR
            String line = value.toString().trim();
            String[] pageSubRank = line.split("\t");
            double subRank = Double.parseDouble(pageSubRank[1]);
            context.write(new Text(pageSubRank[0]), new DoubleWritable(subRank));
        }
    }


    public static class SumReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key,
                           Iterable<DoubleWritable> values,
                           Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable value : values) {
                sum = sum + value.get();
            }
            DecimalFormat df = new DecimalFormat("#.00000");
            sum = Double.valueOf(df.format(sum));
            context.write(key, new DoubleWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(UnitSum.class);

        job.setMapperClass(PassMap.class);
        job.setReducerClass(SumReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}