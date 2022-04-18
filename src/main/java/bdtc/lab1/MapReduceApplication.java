package bdtc.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;


@Log4j
public class MapReduceApplication {
    static Path outputDirectory;

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            throw new RuntimeException("You should specify input and output folders!");
        }
        Configuration conf = new Configuration();
        // задаём выходной файл, разделенный запятыми - формат CSV в соответствии с заданием
        conf.set("mapreduce.output.textoutputformat.separator", ",");
//        Class<?> codecClass = Class.forName("SnappyCodec");
//        CompressionCodec codec =  (CompressionCodec)
//                ReflectionUtils.newInstance(SnappyCodec.class,conf);
//        CompressionOutputStream outputStream = codec.createOutputStream();
//        IOUtils.copyBytes();

        Job job = Job.getInstance(conf, "browser count");
        job.setJarByClass(MapReduceApplication.class);
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);


        Path outputDirectory = new Path(args[1]);
        Path inputDirectory = new Path(args[0]);
        FileInputFormat.addInputPath(job, inputDirectory);
        FileOutputFormat.setOutputPath(job, outputDirectory);
        // set compressing true
        FileOutputFormat.setCompressOutput(job,true);
        // set compressing codec
        FileOutputFormat.getOutputCompressorClass(job,SnappyCodec.class);
        // set output file format
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
        // проверяем статистику по счётчикам
        Counter counter = job.getCounters().findCounter(CounterType.ERR);
        log.info("=====================COUNTERS " + counter.getName() + ": " + counter.getValue() + "=====================");
        readSequenceSnappyFile(outputDirectory,conf);
    }
    public static void readSequenceSnappyFile(Path seqFile,Configuration conf) throws IOException {
        //seqFile = MapReduceApplication.outputDirectory;
        try(SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(seqFile),
                SequenceFile.Reader.bufferSize(1024*8))
        ){
            Text key = new Text();
            Text value = new Text();
            while(reader.next(key,value)){
                System.out.println("****"+key+"****");
                System.out.println(value);
                System.out.println("********");
            }
        }
    }
}
