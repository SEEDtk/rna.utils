/**
 *
 */
package org.theseed.reports;

/**
 * This report simply outputs the frequency values computed for the different correlation
 * buckets.  It is presumed it will be graphed and the graph analyzed visually.
 *
 * @author Bruce Parrello
 *
 */
public class RawFreqReporter extends CorrFreqReporter {

    // FIELDS
    /** previous actual value */
    private double oldActual;
    /** previous expected value */
    private double oldExpected;
    /** controlling command processor */
    private IParms processor;

    /**
     * Create the reporter.
     *
     * @param processor		controlling command processor
     */
    public RawFreqReporter(IParms processor) {
        this.processor = processor;
    }

    @Override
    protected void writeHeader() {
        this.println("limit\texpected\tactual\texpected_cum\tactual_cum");
        this.oldActual = 0.0;
        this.oldExpected = this.processor.getBaseExpected();
    }

    @Override
    public void reportBucket(double xValue, double expected, double actual) {
        double deltaExpected = expected - oldExpected;
        double deltaActual = actual - oldActual;
        this.formatln("%6.4f\t%6.4f\t%6.4f\t%6.4f\t%6.4f", xValue, deltaExpected, deltaActual,
                expected, actual);
        this.oldActual = actual;
        this.oldExpected = expected;
    }

    @Override
    public void closeReport() {
    }

}
