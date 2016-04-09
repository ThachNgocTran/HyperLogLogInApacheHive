package com.mycompany;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.Locale;

/* HOW TO USE IN HIVE: Supposed that "ip" field is of String type.

add jar hdfs://[some address where Hive can find...]/HyperLogLogSynopsis.jar;

create temporary function HyperLogLogSynopsis as 'com.mycompany.HyperLogLogSynopsis';

INSERT OVERWRITE TABLE hllSynopsis
SELECT t.dt, HyperLogLogSynopsis(t.ip)
FROM myiptable t
WHERE  t.dt IN ('20141201', '20141202', '20141203', '20141204',
                     '20141205', '20141206', '20141207')
GROUP  BY t.dt
ORDER  BY t.dt ASC
*/

public class HyperLogLogSynopsis extends AbstractGenericUDAFResolver {

    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1){
            throw new UDFArgumentTypeException(0, "Specify exactly one argument.");
        }

        // Check if the only input is of string (Primitive type in Java)
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only string type argument is accepted.");
        }

        // Check again with casting.
        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case STRING:
                break;
            default:
                throw new UDFArgumentTypeException(0, "Only string type argument is accepted.");
        }

        // Everything is OK now.
        return new HyperLogLogSynopsisEvaluator();
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public static class HyperLogLogSynopsisEvaluator extends GenericUDAFEvaluator {
        /// These share function iterate()
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private PrimitiveObjectInspector inputOI;   // this should be string

        /// These share function merge()
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list of bytes)
        private transient ListObjectInspector loi;  // this should be array of bytes

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            /// This is of "utmost importance"; otherwise, strange problems happen!!!
            super.init(m, parameters);

            /// INPUT!!!
            // init input object inspectors
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                /// These share iterate()
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                /// These share merge()
                loi = (ListObjectInspector) parameters[0];
            }

            /// OUTPUT!!!
            // initialize output object inspectors
            return ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.writableByteObjectInspector);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            HyperLogLogBuffer hllbuff = (HyperLogLogBuffer)agg;

            ArrayList<ByteWritable> result = new ArrayList<ByteWritable>();

            /// Maybe not too optimized because converting from array of bytes to List of ByteWritable???
            try{
                //byte[] arrBytes = testSerialize.toByteArray(hllbuff.hllp);
                byte[] arrBytes = testSerialize.toByteArray2(hllbuff.hllp, true);
                for(int i = 0; i < arrBytes.length; i++){
                    result.add(new ByteWritable(arrBytes[i]));
                }
            }
            catch (Exception ex){
                // There shouldn't be any errors of any kind here; thus, stop the task!
                throw new HiveException(String.format(Locale.ENGLISH, "MyError: %s", ex.toString()));
            }

            return result;
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            /// The final result or the intermediate result are the same (the array of bytes representing the HyperLogLog instance)
            return terminatePartial(agg);
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            HyperLogLogBuffer hllBuff = (HyperLogLogBuffer)agg;

            String cookie = PrimitiveObjectInspectorUtils.getString(parameters[0], inputOI);

            /// Digest the new data! This is the iterate() function.
            if (cookie != null && cookie.length() > 0 && !cookie.equals("-"))
                hllBuff.hllp.offer(cookie);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null)
                return;

            ArrayList<ByteWritable> partialHllData = (ArrayList<ByteWritable>)loi.getList(partial);
            ByteObjectInspector objIns = (ByteObjectInspector)loi.getListElementObjectInspector();

            byte[] partialHllArr = new byte[partialHllData.size()];
            for(int i = 0; i < partialHllArr.length; i++){
                ByteWritable bwt = partialHllData.get(i);
                /// In "NumericHistogram.java", this works the same way: use ObjectInspector to extract the value.
                /// Notice that there are 2 ByteWritable versions:
                /// https://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/ByteWritable.html
                /// https://hive.apache.org/javadocs/r0.12.0/api/org/apache/hadoop/hive/serde2/io/ByteWritable.html
                /// https://github.com/apache/hive/blob/master/serde/src/java/org/apache/hadoop/hive/serde2/io/ByteWritable.java
                partialHllArr[i] = objIns.get(bwt);
                // The other way is: bwt.get()
                /// Probably above is the best we can do (https://stackoverflow.com/questions/21106146/read-values-wrapped-in-hadoop-arraywritable)
            }

            HyperLogLogPlus partialHll = null;

            try{
                //partialHll = testSerialize.fromByteArray(partialHllArr);
                partialHll = testSerialize.fromByteArray2(partialHllArr, true);
            }
            catch(Exception ex){
                throw new HiveException(String.format(Locale.ENGLISH, "MyError: %s", ex.toString()));
            }

            HyperLogLogBuffer hllBuff = (HyperLogLogBuffer)agg;
            try{
                /// Digest the bitfields from another HyperLogLog instance.
                hllBuff.hllp.addAll(partialHll);
            }
            catch (Exception ex){
                throw new HiveException(String.format(Locale.ENGLISH, "MyError: %s", ex.toString()));
            }
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            HyperLogLogBuffer result = new HyperLogLogBuffer();
            reset(result);
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            HyperLogLogBuffer result = (HyperLogLogBuffer)agg;
            result.hllp = new HyperLogLogPlus(16);  // Use HLL 16 bits! Hardcoded!!!
        }

        // Aggregation buffer definition and manipulation methods
        @AggregationType(estimable = true)
        static class HyperLogLogBuffer extends AbstractAggregationBuffer {
            HyperLogLogPlus hllp;   // the HyperLogLog object
        };
    }
}
