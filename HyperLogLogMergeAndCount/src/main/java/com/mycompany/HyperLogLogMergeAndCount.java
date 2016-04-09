package com.mycompany;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyByte;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.Locale;

/* HOW TO USE IN HIVE: Supposed that "hll" field is of Byte Array type.
-- Calculate the cardinality from the HLL synopses.
add jar hdfs://[some address where Hive can find...]/HyperLogLogMergeAndCount.jar;

create temporary function HyperLogLogMergeAndCount as 'com.mycompany.HyperLogLogMergeAndCount';

SELECT HyperLogLogMergeAndCount(t.hll)
FROM hllsynopsis t
WHERE t.dt IN ( '20141201', '20141202', '20141203', '20141204', 
                     '20141205', '20141206', '20141207')
*/

public class HyperLogLogMergeAndCount extends AbstractGenericUDAFResolver {

    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1){
            throw new UDFArgumentTypeException(0, "Specify exactly one argument.");
        }

        // Check if the only input is of array
        if (parameters[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(0, "Only array type argument is accepted.");
        }

        // Check if the element of the array is of tiny int (byte)
        TypeInfo ele = ((ListTypeInfo) parameters[0]).getListElementTypeInfo();
        if (ele.getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentTypeException(0, "Only array<tinyint> type argument is accepted.");
        }

        if (((PrimitiveTypeInfo)ele).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.BYTE){
            throw new UDFArgumentTypeException(0, "Only array<tinyint> type argument is accepted.");
        }

        // Everything is OK now.
        return new HyperLogLogMergeAndCountEvaluator();
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public static class HyperLogLogMergeAndCountEvaluator extends GenericUDAFEvaluator {
        private ListObjectInspector inputOI;   // this should be array of bytes

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            /// This is of "utmost importance"; otherwise, strange problems happen!!!
            super.init(m, parameters);

            /// INPUT!!!
            // init input object inspectors
            inputOI = (ListObjectInspector) parameters[0];

            /// OUTPUT!!!
            // initialize output object inspectors
            if (m == Mode.FINAL || m == Mode.COMPLETE) {
                /// return one integer number
                /// Difference between these below???
                //PrimitiveObjectInspectorFactory.javaIntObjectInspector;
                //PrimitiveObjectInspectorFactory.writableIntObjectInspector
                return ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,
                        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            } else { // (m == Mode.PARTIAL1 || m == Mode.PARTIAL2)
                /// return byte array
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableByteObjectInspector);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            HyperLogLogBuffer hllbuff = (HyperLogLogBuffer)agg;
            ArrayList<ByteWritable> result = new ArrayList<ByteWritable>();

            try{
                //byte[] arrBytes = testSerialize.toByteArray(hllbuff.hllp);
                byte[] arrBytes = testSerialize.toByteArray2(hllbuff.hllp, true);
                for(int i = 0; i < arrBytes.length; i++){
                    result.add(new ByteWritable(arrBytes[i]));
                }
            }
            catch (Exception ex){
                throw new HiveException(String.format(Locale.ENGLISH, "MyError: %s", ex.toString()));
            }

            return result;
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            HyperLogLogBuffer hllbuff = (HyperLogLogBuffer)agg;

            /// return the estimated cardinality!
            return (int)hllbuff.hllp.cardinality();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            merge(agg, parameters[0]);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            HyperLogLogBuffer hllBuff = (HyperLogLogBuffer)agg;

            //ArrayList<ByteWritable> partialHllData = (ArrayList<ByteWritable>)inputOI.getList(partial);
            ArrayList<Object> partialHllData = (ArrayList<Object>)inputOI.getList(partial);
            ByteObjectInspector objIns = (ByteObjectInspector)inputOI.getListElementObjectInspector();

            byte[] partialHllArr = new byte[partialHllData.size()];

            for(int i = 0; i < partialHllArr.length; i++){
                //ByteWritable bwt = partialHllData.get(i);
                Object tempObj = partialHllData.get(i);
                ByteWritable bwt = null;

                // It's probably that when reading data from table, they are of LazyByte (storing even null!); but intermediate data in shuffle phase are of WritableByte. (transferring...)
                if (tempObj instanceof LazyByte)
                    bwt = ((LazyByte)tempObj).getWritableObject();
                else    // must be!!!
                    bwt = (ByteWritable)tempObj;

                //partialHllArr[i] = objIns.get(bwt);
                partialHllArr[i] = bwt.get();
            }

            HyperLogLogPlus partialHll = null;

            try{
                //partialHll = testSerialize.fromByteArray(partialHllArr);
                partialHll = testSerialize.fromByteArray2(partialHllArr, true);
            }
            catch(Exception ex){
                throw new HiveException(String.format(Locale.ENGLISH, "MyError: %s", ex.toString()));
            }

            try{
                /// "Digest" the bitfields of another HyperLogLog instance.
                hllBuff.hllp.addAll(partialHll);
            }
            catch (Exception ex){
                throw new HiveException(String.format(Locale.ENGLISH, "MyError: %s", ex.toString()));
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            HyperLogLogBuffer result = new HyperLogLogBuffer();
            reset(result);
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            HyperLogLogBuffer result = (HyperLogLogBuffer)agg;
            result.hllp = new HyperLogLogPlus(16);  // Use HLL 16 bits!
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Aggregation buffer definition and manipulation methods
        @AggregationType(estimable = true)
        static class HyperLogLogBuffer extends AbstractAggregationBuffer {
            HyperLogLogPlus hllp;   // the HyperLogLog object
        }
    }
}
