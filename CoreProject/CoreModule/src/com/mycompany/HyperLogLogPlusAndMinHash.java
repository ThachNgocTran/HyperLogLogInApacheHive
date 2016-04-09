package com.mycompany;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import java.io.Serializable;
import java.util.TreeSet;

/*
Reference Code: https://github.com/AdRoll/cantor/blob/master/src/main/java/com/adroll/cantor/HLLCounter.java
 */

// We can serialize this class.
public class HyperLogLogPlusAndMinHash implements Serializable {

    // for better management and not causing a mess!
    private HyperLogLogPlus hllp = null;
    private int p;  // once an HLL instance is created, we cannot get its p.

    /** Default HLL precision of 18 */
    public static final byte DEFAULT_P = (byte)18;

    /** Default MinHash precision of 8192 if intersectable
     and HLL precision is </code>DEFAULT_P</code>
     */
    public static final int DEFAULT_K = 8192;

    /** MinHash structure */
    private TreeSet<Long> ts;
    /** precision of MinHash structure */
    private int k;

    public HyperLogLogPlusAndMinHash(int p, int k){
        this.p = p;
        this.hllp = new HyperLogLogPlus(this.p);
        this.k = k;
        this.ts = new TreeSet<Long>();
    }

    public HyperLogLogPlusAndMinHash(){
        this(DEFAULT_P, DEFAULT_K);
    }

    // Similarly to HyperLogLogPlus but additionally store the hashed value to the tree.
    public void offer(Object v) {
        long x = MurmurHash.hash64(v);

        this.ts.add(x);
        if(this.ts.size() > this.k) {
            this.ts.pollLast();     // remove the largest item
        }

        this.hllp.offerHashed(x);
    }

    public long cardinality(){
        // we can check if (ts.size() < k), then return ts.size() ==> Exact Method!!!!!
        // TreeSet is helpful only when making the intersection.

        return this.hllp.cardinality();
    }

    // Absorb data from another HyperLogLogPlusAndMinHash instance
    // It is expected that the other HyperLogLogPlusAndMinHash has the same k and p.
    public void addAll(HyperLogLogPlusAndMinHash other) throws Exception{
        this.hllp.addAll(other.hllp);
        this.ts.addAll(other.ts);

        int mustReduce = this.ts.size() - this.k;
        for(int i=0; i < mustReduce; i++){
            this.ts.pollLast();
        }
    }

    // Create a new HyperLogLogPlusAndMinHash, absorb data from THIS instance and ALL OTHER instances.
    // It is expected that all other HyperLogLogPlusAndMinHash have the same k and p.
    public HyperLogLogPlusAndMinHash merge(HyperLogLogPlusAndMinHash... estimators) throws Exception{
        HyperLogLogPlusAndMinHash merged = new HyperLogLogPlusAndMinHash(this.p, this.k);
        merged.addAll(this);

        /// If there is no other HLL instance.
        if (estimators == null) {
            return merged;
        }

        for (HyperLogLogPlusAndMinHash estimator : estimators) {
            merged.addAll(estimator);
        }

        return merged;
    }

    // This is a STATIC method.
    // This function is to return the Jaccard Index of many HyperLogLogPlusAndMinHash structures.
    public static double getJaccardIndex(HyperLogLogPlusAndMinHash... hs){
        //We can't actually intersect HLLCounters, but we
        //can provide an estimate of the size of the
        //intersection using the MinHash algorithm.
        if (hs.length == 0) {
            return 0;
        }

        // If there is one set equal to zero, the intersection is always zero.
        for (HyperLogLogPlusAndMinHash hllpmh : hs) {
            if (hllpmh.getMinHash().size() == 0) {
                return 0;
            }
        }

        // Process the trees...
        TreeSet<Long> allTrees = new TreeSet<Long>();
        int mink = Integer.MAX_VALUE;
        int maxs = Integer.MIN_VALUE;

        for(HyperLogLogPlusAndMinHash h : hs) {
            allTrees.addAll(h.getMinHash());
            mink = Math.min(mink, h.getK());	            // Get the Smallest K (maybe redundant as we expect they all have the same k and p)
            maxs = Math.max(maxs, h.getMinHash().size());   // Get the Biggest Tree Size
        }

        mink = maxs < mink ? maxs : mink;   // If biggest tree size < smallest K ==> Use biggest tree size
        // If smallest K < biggest tree size ==> Use smallest K

        int result = 0;
        for(int i = 0; i < mink; i++) {
            long l = 0;
            try {
                l = allTrees.pollFirst(); // Should we use the elements of the smallest set???
            } catch(NullPointerException e) {
                //This can happen if k is larger than
                //the number of insertions.
                break;
            }
            boolean allContain = true;
            for(HyperLogLogPlusAndMinHash h : hs) {
                if(!h.getMinHash().contains(l)) {
                    allContain = false;
                    break;
                }
            }
            if(allContain) {
                result += 1;
            }
        }

        return (((double)result)/((double)mink));
    }

    // This is a STATIC method.
    public static long intersect(HyperLogLogPlusAndMinHash... hs) throws Exception{

        double jcIn = getJaccardIndex(hs);
        if (jcIn == 0)  // decide immediately
            return 0;

        // Process the HyperLogLogPlus instances...
        HyperLogLogPlus hllpFirst = hs[0].hllp;
        HyperLogLogPlus[] hllpTheRest = new HyperLogLogPlus[hs.length - 1];
        for(int i = 1; i < hs.length; i++){
            hllpTheRest[i-1] = hs[i].hllp;
        }

        HyperLogLogPlus hllpAll = (HyperLogLogPlus)hllpFirst.merge(hllpTheRest);

        // Calculate the intersection:
        return (long)Math.round(jcIn * hllpAll.cardinality());
    }

    public TreeSet<Long> getMinHash() {
        return ts;
    }

    public int getK() {
        return k;
    }
}

