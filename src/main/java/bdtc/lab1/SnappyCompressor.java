package bdtc.lab1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.SnappyCodec;

public class SnappyCompressor {

    Class<?> codecClass = Class.forName("SnappyCodec");

    public SnappyCompressor() throws ClassNotFoundException {
    }
}
