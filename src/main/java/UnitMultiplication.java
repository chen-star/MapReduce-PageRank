
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

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


            // input: fromPage\ttoPage1,toPage2,toPage3
            // target: build transition matrix unit -> fromPage\ttoPage=probability
            String[] strs = value.toString().trim().split("\t");
            if(strs.length < 2) {
                return;
            }
            String outputkey = strs[0];
            String[] tos = strs[1].split(",");
            int count = tos.length;

            for (String to : tos){
                context.write(new Text(outputkey), new Text(to + "=" + (double)1/count));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // input: Page\tPageRank
            // target: write to reducer
            String[] pr = value.toString().trim().split("\t");
            if (pr.length < 2) {
                return;
            }
            String outputKey = pr[0];
            String outputValue = pr[1];
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // input: key = fromPage value=<toPage1=probability1....,PageRank>

            // separate transition cell from pr cell
            List<String> transitionCell = new ArrayList<String>();
            double pk = 1;
            for (Text value : values) {
                String str = value.toString().trim();
                //context.getCounter("PRValue", str);
                if (str.indexOf("=") == -1) {
                    pk = Double.parseDouble(str);
                } else {
                    transitionCell.add(str);
                }
            }

            //multiplication
            for (String text : transitionCell) {
                String[] strs = text.split("=");
                if(strs.length == 2) {
                    // topage
                    String topage = strs[0];
                    // probability
                    double probability = Double.parseDouble(strs[1]);
                    context.write(new Text(topage), new Text(String.valueOf(probability * pk)));
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job;
        job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
