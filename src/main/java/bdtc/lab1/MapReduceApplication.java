package bdtc.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


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
//    public static File hdfsFileToLocalFile(Path some_path, Configuration conf) throws IOException {
//        ;
//
//        fs.copyToLocalFile(some_path, new Path(temp_data_file.getAbsolutePath()));
//        return temp_data_file;
//    }
    public static boolean isHidden(java.nio.file.Path path){
        try {
            return Files.isHidden(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
    public static void readSequenceSnappyFile(Path seqFiles,Configuration conf) throws IOException {
        int i = 0;
        while(i<2){
            i++;
            List<Path> filesInFolder;
            try {
                filesInFolder = Collections.singletonList((Path) Files.list(Paths.get(seqFiles.toUri()))
                        .filter(Files::isRegularFile)
                        .filter(MapReduceApplication::isHidden)
                        .collect(Collectors.toList()));
            } catch(IOException exception){
                log.error("Cannot extract files as Java objects from directory ",exception);
            }
            try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file((Path) Files.list(Paths.get(seqFiles.toUri()))
                            .filter(Files::isRegularFile)
                            .filter(MapReduceApplication::isHidden)
                            .collect(Collectors.toList()).get(i)),
                    SequenceFile.Reader.bufferSize(1024 * 8))
            ) {
                Text key = new Text();
                Text value = new Text();
                while (reader.next(key, value)) {
                    System.out.println("****" + key + "****");
                    System.out.println(value);
                    System.out.println("********");
                }
            }
        }
    }
}
