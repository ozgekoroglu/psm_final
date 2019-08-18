package org.processmininginlogistics.classifiers.bp.example;

import org.processmining.scala.log.common.enhancment.segments.common.AbstractDurationClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * README
 * 1. Use the PSM jar as a dependency and implement your classifier.
 * It must implement AbstractDurationClassifier and has a constructor with a single String argument.
 * This argument is an event log(s) path, or an empty string in case of using imported XES log in ProM
 * 2. Provide a readable name of the classifier in toString()
 * 3. Compile your implementation as a separate jar
 * 4. Put it into the CLASSPATH of the PSM
 * 5. Run the PSM
 * 6. Provide a full class name in field 'custom classifier' in the pre-processing dialog
 *     e.g. org.processmininginlogistics.classifiers.bp.example.SegmentClassifierExample
 * 7. Check the legend in the PSM. It should show your classifier name and classes
 * <p>
 * If you work with the PSM from an IDE, it may be easier to implement your classifier as a part of the PSM.
 * Do not commit non-generic or containing proprietary information classifiers to the PSM!
 */
public class SegmentClassifierExample implements AbstractDurationClassifier {
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.US); //Specify your locale

    private static final Logger logger = LoggerFactory.getLogger(SegmentClassifierExample.class.getName());

    class MyKey {
        String caseId;
        String segmentName;
        Long timestamp; // UNIX time UTC timezone in ms

        public MyKey(String caseId, String segmentName, Long timestamp) {
            this.caseId = caseId;
            this.segmentName = segmentName;
            this.timestamp = timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MyKey myKey = (MyKey) o;
            return Objects.equals(caseId, myKey.caseId) &&
                    Objects.equals(segmentName, myKey.segmentName) &&
                    Objects.equals(timestamp, myKey.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(caseId, segmentName, timestamp);
        }
    }

    Map<MyKey, Double> map_outlier = new HashMap<>();
    Double median_blockage;
    public SegmentClassifierExample(final String path) {
        String line = "";
        String cvsSplitBy = ",";
        // outlierList = new ArrayList<OutlierInformation>();
        //path to description
        String dirName = (new File(path)).getParent();
        //File[] csvFiles = (new File(dirName)).listFiles(x -> x.getPath().toLowerCase().endsWith("csv"));
        String filename = String.format("%s/outliers.txt", dirName);//  csvFiles[0].getPath();

        int lineCount = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {

            while ((line = br.readLine()) != null) {
                lineCount++;
                if (lineCount > 1) {

                    // use comma as separator
                    String[] file = line.split(cvsSplitBy);
                    median_blockage =Double.valueOf(file[9]);
                        Double blockage_time = -1.0;
                        long unixTime = 0l;
                        try {
                            dateFormat.setTimeZone(TimeZone.getTimeZone("Europe/Amsterdam"));
                            unixTime = dateFormat.parse(file[0]).getTime();
                            if (file[8].equals("1")) {
                                blockage_time = Double.valueOf(file[7]);
                            }
                        } catch (NumberFormatException e) {
                            e.printStackTrace();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        MyKey outlier_key = new MyKey(file[2], file[1], unixTime);

                        map_outlier.put(outlier_key, blockage_time);
                    }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int classify(long duration, double q2, double median, double q4, String caseId, long timestamp, String segmentName, double medianAbsDeviation) {
        MyKey csv_key = new MyKey(caseId, segmentName, timestamp);
        Double clazz =  map_outlier.get(csv_key);
        int ret = 3;
        if(clazz != null) {
            if (clazz >= median_blockage)
                ret = 1;
            else if ( clazz > 0.0)
                ret = 0;
            else
                ret =  2;
        }
//        if (clazz != null) {
//            logger.info(String.format("key='%s-%s-%d' clazz=%f psmClass=%d", caseId, segmentName, timestamp, clazz, ret));
//        }
        return ret;
    }

    @Override
    public String sparkSqlExpression(String attrNameDuration, String attrNameClazz) {
        throw new NotImplementedException();
    }

    @Override
    public String legend() {
        return "Blockage%Normal%Serious%NoBlockage%None";
    }

    @Override
    public int classCount() {
        return 4;
    }

    @Override
    public String toString() {
        return "Blockage";
    }

       public static void main(String args[]){
        new SegmentClassifierExample("C:\\Users\\nlokor\\Desktop\\New folder\\x.csv");
    }
}