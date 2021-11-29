/**
 *
 */
package org.theseed.rna.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.TabbedLineReader;
import org.theseed.reports.CorrFreqReporter;
import org.theseed.utils.BaseReportProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This command accepts as input a set of pearson correlations.  We wish to analyze the
 * positive pearson correlations to determine the point at which the higher correlations
 * begin to occur with abnormal frequency.  To do this, we assume a normal distribution
 * of correlations around 0.  The actual cumulative frequency is then compared to the
 * expected cumulative frequency at each point.
 *
 * For reporting purposes, we will divide the pearson range (-1 to 1) into a specified
 * number of buckets.  We compare the number of observations to the left of each bucket
 * with the expected cumulative number and focus on the difference.
 *
 * The input file will be read from the standard input.
 *
 * The output report will be written to the standard output.  This is a text report, not
 * a table.
 *
 * The positional parameter is the name of the column containing the pearson coefficients.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file (if not STDIN)
 * -o	output file for report (if not STDOUT)
 * -q	number of buckets into which the range should be divided (default 100)
 *
 * --format		output format for the report (default RAW)
 *
 *
 * @author Bruce Parrello
 *
 */
public class CorrFreqProcessor extends BaseReportProcessor implements CorrFreqReporter.IParms {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(CorrFreqProcessor.class);
    /** width of a bucket */
    private double bucketWidth;
    /** array of bucket counts */
    private int[] buckets;
    /** array of bucket limits:  bucket[i] contains values <= limits[i] */
    private double[] limits;
    /** statistics object for computing mean and deviation */
    private SummaryStatistics stats;
    /** reporting object for output */
    private CorrFreqReporter reporter;
    /** index of the input column */
    private int inColIdx;
    /** number of input observations */
    private double obsCount;
    /** input stream */
    private TabbedLineReader inStream;
    /** this is the base expectation value for histograms */
    private double baseExpected;

    // COMMAND-LINE OPTIONS

    /** input file (if not STDIN) */
    @Option(name = "--input", aliases = { "-i" }, metaVar = "inFile.tbl", usage = "input file (if not STDIN)")
    private File inFile;

    /** number of buckets */
    @Option(name = "--buckets", aliases = { "-q" }, metaVar = "50", usage = "number of buckets in which to divide the range")
    private int nBuckets;

    /** type of output report */
    @Option(name = "--format", usage = "format for output report")
    private CorrFreqReporter.Type reportType;

    /** index (1-based) or name of input column */
    @Argument(index = 0, metaVar = "colName", usage = "index (1-based) or name of input column", required = true)
    private String colName;

    @Override
    protected void setReporterDefaults() {
        this.inFile = null;
        this.nBuckets = 100;
        this.reportType = CorrFreqReporter.Type.RAW;
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        // Compute the bucket width.
        if (this.nBuckets < 10)
            throw new ParseFailureException("Bucket count (q) must be 10 or more.");
        this.bucketWidth = 2.0 / this.nBuckets;
        log.info("Bucket width is {}.", this.bucketWidth);
        // Allocate the bucket arrays.
        this.buckets = new int[this.nBuckets];
        this.limits = new double[this.nBuckets];
        // Initialize the arrays.  Note we multiply here to reduce the accumulation of roundoff.
        for (int i = 0; i < this.nBuckets; i++) {
            this.limits[i] = (i + 1) * this.bucketWidth - 1.0;
            this.buckets[i] = 0;
        }
        // Create the output reporter.
        this.reporter = this.reportType.create(this);
        // Create the statistics object.
        this.stats = new SummaryStatistics();
        // Open the input stream.
        if (this.inFile == null) {
            log.info("Correlations will be read from the standard input.");
            this.inStream = new TabbedLineReader(System.in);
        } else {
            log.info("Correlations will be read from {}.", this.inFile);
            this.inStream = new TabbedLineReader(this.inFile);
        }
        // Find the input column index.
        this.inColIdx = this.inStream.findField(this.colName);
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        try {
            // Loop through the input file, accumulating observations in buckets.
            int count = 0;
            int errors = 0;
            for (TabbedLineReader.Line line : this.inStream) {
                double value = line.getDouble(this.inColIdx);
                // We have to eliminate non-finite values:  they mess everything up.
                if (! Double.isFinite(value))
                    errors++;
                else {
                    int idx = (int) ((value + 1.0) / this.bucketWidth);
                    // Insure the input value is valid.
                    if (idx > this.nBuckets || idx < 0) {
                        log.warn("Value {} out of range.", value);
                        errors++;
                    } else {
                        // We need to adjust the array index.  The distribution service computes
                        // probability <= X, so if we are at the limit of the previous bucket,
                        // we subtract 1.
                        if (idx > 0 && value <= this.limits[idx-1]) idx--;
                        // Store this value in the bucket.
                        this.buckets[idx]++;
                        count++;
                        if (count % 5000 == 0)
                            log.info("{} observations processed.", count);
                        // Record it in the stats object.
                        this.stats.addValue(value);
                    }
                }
            }
            log.info("{} total observations, {} errors.", count, errors);
            this.obsCount = (double) count;
            // Compute the mean and standard devation.
            double mean = this.stats.getMean();
            double sdev = this.stats.getStandardDeviation();
            log.info("Mean is {}, standard deviation is {}.", mean, sdev);
            // We compute a distribution with a mean of 0 and the specified standard
            // deviation.
            NormalDistribution dist = new NormalDistribution(null, 0.0, sdev);
            // Save the base expectation value.
            this.baseExpected = dist.cumulativeProbability(-1.0);
            // Here we will accumulate the total of the buckets processed so far.
            int cum = 0;
            // Now we write the reports for the buckets.
            this.reporter.openReport(writer);
            for (int i = 0; i < this.nBuckets; i++) {
                cum += this.buckets[i];
                double actual = cum / this.obsCount;
                double expected = dist.cumulativeProbability(this.limits[i]);
                this.reporter.reportBucket(this.limits[i], expected, actual);
            }
            // Finish the report.
            this.reporter.closeReport();
        } finally {
            if (this.inStream != null)
                inStream.close();
        }

    }

    @Override
    public double getBaseExpected() {
        return this.baseExpected;
    }

}
