package org.processmininginlogistics.classifiers.bp.example;

import org.processmining.scala.log.common.enhancment.segments.common.AbstractDurationClassifier;
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
    //private final static DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH.mm.ss", Locale.US);
    private DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH.mm.ss", Locale.US); //Specify your locale

    class MyKey{
        String caseId;
        String segmentName;
        Long timestamp; // UNIX time UTC timezone in ms


        public MyKey(String caseId, String segmentName, Long timestamp ) {
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
    Map<MyKey, Integer> map_outlier= new HashMap<>();

    public SegmentClassifierExample(final String path) {

        String line = "";
        String cvsSplitBy = ",";
       // outlierList = new ArrayList<OutlierInformation>();
        //path to description
        String dirName = (new File(path)).getParent();
        File[] csvFiles = (new File(dirName)).listFiles(x -> x.getPath().toLowerCase().endsWith("csv"));
        String filename = csvFiles[0].getPath();

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {

            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] file = line.split(cvsSplitBy);
                int outlierType = -1;
                long unixTime = 0;
                String date=file[0];
                try {
                    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT+1"));
                    unixTime = dateFormat.parse(date).getTime();
                    outlierType = Double.valueOf(file[7]).intValue();
                }
                catch (NumberFormatException e){} catch (ParseException e) {
                    e.printStackTrace();
                }
                MyKey outlier_key =new MyKey(file[2],file[1],unixTime);
                map_outlier.put(outlier_key,outlierType);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int classify(long duration, double q2, double median, double q4, String caseId, long timestamp, String segmentName, double medianAbsDeviation) {
        try {
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT+1"));
            timestamp = dateFormat.parse(Long.toString(timestamp)).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        MyKey csv_key =new MyKey(caseId,segmentName,timestamp);
       return  map_outlier.get(csv_key);
    }

    @Override
    public String sparkSqlExpression(String attrNameDuration, String attrNameClazz) {
        throw new NotImplementedException();
    }

    @Override
    public String legend() {
        return "Example%Class_0%Class_1";
    }

    @Override
    public int classCount() {
        return 2;
    }

    @Override
    public String toString() {
        return "Segment/case classifier example";
    }
}
