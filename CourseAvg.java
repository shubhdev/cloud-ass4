import java.io.IOException;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CourseAvg {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
    private IntWritable score = new IntWritable();
    private Text course = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      int i = 0;
      while (itr.hasMoreTokens()) {
	if(i == 0) {
	  itr.nextToken();	
	}
	if(i == 1) {
	  course.set(itr.nextToken());
	}
	if(i==2) {
	  score.set(Integer.parseInt(itr.nextToken()));
	}
	i += 1;
      }
     context.write(course, score);
     //TimeUnit.MICROSECONDS.sleep(250);
    }
  }

  public static class IntAvgReducer
       extends Reducer<Text,IntWritable,Text,FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      int size = 0;
      for (IntWritable val : values) {
        sum += val.get();
	size += 1;
      }       
      //TimeUnit.MICROSECONDS.sleep(250);
      float avg = sum;
      avg /= size;
      result.set(avg);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "course average");
    job.setJarByClass(CourseAvg.class);
    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntAvgReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
