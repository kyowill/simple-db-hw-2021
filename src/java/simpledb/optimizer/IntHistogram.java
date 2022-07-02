package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int min;

    private int max;

    private int[] buckets;

    private float width;

    private static float epsilon = 0.00001f;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = new int[buckets];
        this.max = max;
        this.min = min;
        this.width = (max - min) * 1.0f / buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int idx = getIndex(v);
        if (idx == -1) {
            return;
        }
        if (idx == buckets.length) {
            return;
        }
        buckets[idx]++;
    }

    private int getIndex(int v) {
        if (v < min) {
            return -1;
        }
        if (v > max) {
            return buckets.length;
        }
        if (v == max) {
            return buckets.length - 1;
        }
        float sum = (float) min; // 防止循环条件隐式类型转换
        float target = (float) v;
        float step = (target - sum) / width;
        if (Math.abs(target - sum + width * step) <= epsilon) {
            return (int) (step - 1);
        }
        return (int) step;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        // some code goes here
        int idx = getIndex(v);
        int sum = 0;
        for (int i = 0; i < buckets.length; i++) {
            sum += buckets[i];
        }
        sum = Math.max(1, sum);
        if (op.equals(Predicate.Op.EQUALS)) {
            if (idx == -1 || idx == buckets.length) {
                return 0.0;
            }
            return (1.0f * buckets[idx]) / sum;
        } else if (op.equals(Predicate.Op.GREATER_THAN)) {
            if (idx == -1) {
                return 1.0f;
            }
            if (idx == buckets.length) {
                return 0.0;
            }
            float subSum = 0;
            for (int i = idx + 1; i < buckets.length; i++) {
                subSum += buckets[i];
            }
            double fraction = (min + width * (idx + 1) - v) / width;
            subSum += buckets[idx] * fraction;
            return subSum / sum;
        } else if (op.equals(Predicate.Op.LESS_THAN)) {
            if (idx == -1) {
                return 0.0f;
            }
            if (idx == buckets.length) {
                return 1.0;
            }
            float subSum = 0;
            for (int i = 0; i < idx; i++) {
                subSum += buckets[i];
            }
            double fraction = (v - (min + width * idx)) / width;
            subSum += buckets[idx] * fraction;
            return subSum / sum;
        } else if (op.equals(Predicate.Op.NOT_EQUALS)) {
            if (idx == -1 || idx == buckets.length) {
                return 1.0;
            }
            float subSum = 0;
            for (int i = 0; i < buckets.length; i++) {
                if (i == idx) {
                    continue;
                }
                subSum += buckets[i];
            }
            return subSum / sum;
        } else if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            if (idx == -1) {
                return 0.0f;
            }
            if (idx == buckets.length) {
                return 1.0;
            }
            float subSum = 0;
            for (int i = 0; i < idx; i++) {
                subSum += buckets[i];
            }
            subSum += buckets[idx];
            return subSum / sum;
        } else if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            if (idx == -1) {
                return 1.0f;
            }
            if (idx == buckets.length) {
                return 0.0;
            }
            float subSum = 0;
            for (int i = idx + 1; i < buckets.length; i++) {
                subSum += buckets[i];
            }
            subSum += buckets[idx];
            return subSum / sum;
        }
        return -1.0;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return String.format("min:%s, max:%s, width:%s", min, max, width);
    }
}
