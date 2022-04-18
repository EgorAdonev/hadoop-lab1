import eu.bitwalker.useragentutils.UserAgent;
import bdtc.lab1.CounterType;
import bdtc.lab1.HW1Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class CountersTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    private final String testMalformedLogLine = "Apr 17 2022 04:06:01 localhost kernel: amogus: gvfsd-admin uses 32-bit capabilities (legacy)";
    private final String testLogLine = "Apr 17 2022 04:06:01 localhost kernel: 6: gvfsd-admin uses 32-bit capabilities (legacy)";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapperCounterOne() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text(testMalformedLogLine))
                .runTest();
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(CounterType.MALFORM).getValue());
    }

    @Test
    public void testMapperCounterCrit() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testLogLine))
                .withOutput(new Text(parseLineAndReturnSeverity(testLogLine)), new IntWritable(1))
                .runTest();
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(CounterType.CRIT).getValue());
    }

    @Test
    public void testMapperCounters() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testLogLine))
                .withInput(new LongWritable(), new Text(testMalformedLogLine))
                .withInput(new LongWritable(), new Text(testMalformedLogLine))
                .withOutput(new Text(parseLineAndReturnSeverity(testLogLine)), new IntWritable(1))
                .runTest();

        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
                .findCounter(CounterType.MALFORM).getValue());
    }
    //custom parsing
    private String parseLineAndReturnSeverity(String logLine) {
        String[] splitBySpace = logLine.split(" ");
        String severityString = splitBySpace[6];
        return severityString;
    }
}

