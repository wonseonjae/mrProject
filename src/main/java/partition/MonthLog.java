package partition;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@Log4j
public class MonthLog extends Configuration implements Tool {

    public static void main(String[] args) throws Exception{
        if (args.length != 2) {
            log.info("값을 제대로 입력해 주세요");
            System.exit(-1);
        }
        int exitCode = ToolRunner.run(new MonthLog(), args);

        System.exit(exitCode);

    }
    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        String appName = conf.get("AppName");
        log.info("appName : "+appName);

        Job job = Job.getInstance(conf);

        job.addCacheFile(new Path("/access_log").toUri());

        job.setJarByClass(MonthLog.class);
        job.setJobName(appName);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MonthLogMapper.class);
        job.setReducerClass(MonthLogReducer.class);
        job.setPartitionerClass(MonthLogPartitioner.class);

        job.setNumReduceTasks(12);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        boolean success = job.waitForCompletion(true);
        return (success ? 0 : 1);
    }

    @Override
    public void setConf(Configuration configuration) {

        configuration.set("AppName","partitioner test");

    }

    @Override
    public Configuration getConf() {
        Configuration conf = new Configuration();
        this.setConf(conf);
        return conf;
    }
}
