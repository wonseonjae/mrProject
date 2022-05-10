package cache;

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

import java.net.URI;

@Log4j
public class WordCount3 extends Configuration implements Tool {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            log.info("분석결과가 저장될 폴더를 선택해주세요");
            System.exit(-1);
        }
        long startTime = System.nanoTime();
        int exitCode = ToolRunner.run(new WordCount3(), args);
        long endTime = System.nanoTime();

        log.info("Cache Time : "+ (endTime -startTime) + "ns");
    }

    @Override
    public int run(String[] args) throws Exception {

        String analysisFile = "/comedies";

        Configuration conf = this.getConf();
        String appName = conf.get("AppName");

        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCount3.class);
        job.setJobName(appName);

        job.addCacheFile(new Path(analysisFile).toUri());
        URI[] cacheFiles = job.getCacheFiles();

        FileInputFormat.setInputPaths(job, analysisFile);

        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        job.setMapperClass(WordCount3Mapper.class);
        job.setReducerClass(WordCount3Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        boolean success = job.waitForCompletion(true);

        return (success ? 0 : 1);
    }

    @Override
    public void setConf(Configuration configuration) {
        configuration.set("AppName", "Cache Test");
    }

    @Override
    public Configuration getConf() {
        Configuration conf = new Configuration();
        this.setConf(conf);
        return conf;
    }

}
