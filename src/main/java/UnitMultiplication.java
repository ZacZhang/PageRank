import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class UnitMultiplication {

    public static class TransitionMap extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key,
                        Text value,
                        Context context) throws IOException, InterruptedException {
            // 1\t3,5,6,8...
            // key: 1
            // value: (to) xxx = prob
            String line = value.toString().trim();
            String[] fromTo = line.split("\t");

            // dead end
            if (fromTo.length == 1 || fromTo[1].trim().equals("")) {
                return;
            }

            String from = fromTo[0];
            String[] tos = fromTo[1].split(",");
            for (String to : tos) {
                context.write(new Text(from), new Text(to + "=" + (double)1 / tos.length));
            }
        }
    }

    public static class PageRankMap extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key,
                        Text value,
                        Context context) throws IOException, InterruptedException {
            //1\t1
            String line = value.toString().trim();
            String[] pr = line.split("\t");
            context.write(new Text(pr[0]), new Text(pr[1]));
        }
    }

    public static class MultiplicationReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            // key: 1
            // values: <2=1/4, 7=1/4, 8=1/4, 9=1/4, 1>

            // separate transition cell from pr cell
            List<String> transitionCell = new ArrayList<String>();
            double prCell = 0;

            for (Text value : values) {
                if (value.toString().contains("=")) {
                    transitionCell.add(value.toString().trim());
                } else {
                    prCell = Double.parseDouble(value.toString().trim());
                }
            }

            // multiply
            for (String cell : transitionCell) {
                String outputKey = cell.split("=")[0];
                double relation = Double.parseDouble(cell.split("=")[1]);
                String outputValue = String.valueOf(relation * prCell);
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(UnitMultiplication.class);

        ChainMapper.addMapper(job, TransitionMap.class, Object.class, Text.class, Text.class, Text.class, configuration);
        ChainMapper.addMapper(job, PageRankMap.class, Object.class, Text.class, Text.class, Text.class, configuration);

        job.setReducerClass(MultiplicationReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // args[0]: transition.txt
        // args[1]: pr.txt
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMap.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PageRankMap.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}