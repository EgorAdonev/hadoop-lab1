package bdtc.lab1;

import com.github.palindromicity.syslog.SyslogParser;
import com.github.palindromicity.syslog.SyslogParserBuilder;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String syslogLine = value.toString();
        String severityString = parseLineAndReturnSeverity(syslogLine);
//        for (int i = 0;i<splitBySpace.length;i++){
//             String str = splitBySpace[i];
//        }
        //SyslogParser parser = new SyslogParserBuilder().build();
        //Map<String,Object> syslogMap = parser.parseLine(syslogLine);
        //UserAgent userAgent = UserAgent.parseUserAgentString(line);
        if (severityString.equals("1:")) {
            word.set(severityString);
            context.write(word, one);
            context.getCounter(CounterType.DEBUG).increment(1);
        } else if (severityString.equals("2:")) {
            word.set(severityString);
            context.write(word, one);
            context.getCounter(CounterType.INFO).increment(1);
        } else if (severityString.equals("3:")) {
            word.set(severityString);
            context.write(word, one);
            context.getCounter(CounterType.NOTICE).increment(1);
        } else if (severityString.equals("4:")) {
            word.set(severityString);
            context.write(word, one);
            context.getCounter(CounterType.WARN).increment(1);
        } else if (severityString.equals("5:")) {
            word.set(severityString);
            context.write(word, one);
            context.getCounter(CounterType.ERR).increment(1);
        } else if (severityString.equals("6:")) {
            word.set(severityString);
            context.write(word, one);
            context.getCounter(CounterType.CRIT).increment(1);
        } else if (severityString.equals("7:")) {
            word.set(severityString);
            context.write(word, one);
            context.getCounter(CounterType.ALERT).increment(1);
        } else if (severityString.equals("8:")) {
            word.set(severityString);
            context.write(word, one);
            context.getCounter(CounterType.EMERG).increment(1);
        }
        else {
            context.getCounter(CounterType.MALFORM).increment(1);
        }
    }
    public String parseLineAndReturnSeverity(String syslogLine){
        String[] splitBySpace = syslogLine.split(" ");
        String severityString = splitBySpace[6];
        return severityString;
    }
}
