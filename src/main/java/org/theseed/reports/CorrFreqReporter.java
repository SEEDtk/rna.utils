/**
 *
 */
package org.theseed.reports;

/**
 * This is the base reporting class for the correlation frequency report.
 *
 * The unit of reporting in this case is a frequency comparison for a single
 * bucket.
 *
 * @author Bruce Parrello
 *
 */
public abstract class CorrFreqReporter extends BaseReporterReporter {

    /**
     * This interface defines the command-processor attributes needed by the
     * various reports.
     */
    public interface IParms {

        /**
         * @return the expected value for -1.0, for histogram processing
         */
        public double getBaseExpected();

    }

    /**
     * This enumeration specifies the types of reports to write.
     */
    public static enum Type {
        RAW {
            @Override
            public CorrFreqReporter create(IParms processor) {
                return new RawFreqReporter(processor);
            }
        };

        /**
         * @return a reporter of this type
         *
         * @param processor		controlling command processor
         */
        public abstract CorrFreqReporter create(IParms processor);

    }

    /**
     * Report on the frequencies in a bucket.
     *
     * @param	xValue		x-value for the bucket
     * @param	expected	ratio for expected number of observations left of the bucket
     * @param	actual		ratio for actual number of observations left of the bucket
     */
    public abstract void reportBucket(double xValue, double expected, double actual);

    /**
     * Finish the report.
     */
    public abstract void closeReport();

}
