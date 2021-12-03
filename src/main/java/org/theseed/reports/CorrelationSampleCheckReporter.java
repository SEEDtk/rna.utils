/**
 *
 */
package org.theseed.reports;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.theseed.rna.RnaData;

/**
 * This reporter produces a correlation report for all the samples.  The correlation report can be
 * used for clustering.
 *
 * @author Bruce Parrello
 *
 */
public class CorrelationSampleCheckReporter extends SampleCheckReporter {

    // FIELDS
    /** number of lines written */
    private int outputCount;
    /** RNA sample database */
    private RnaData data;
    /** range data for each feature */
    private Map<RnaData.Row, RangeNormalizer> rangeMap;
    /** TRUE if only good samples should be processed */
    private boolean pureMode;

    /**
     * Construct a new correlation report.
     *
     * @param processor		controlling command processor
     */
    public CorrelationSampleCheckReporter(IParms processor) {
        this.pureMode = processor.getPure();
    }

    @Override
    protected void writeHeader() {
        this.println("sample_1\tsample_2\tmean_sim");
    }

    @Override
    public void generate(RnaData data) {
        this.outputCount = 0;
        this.data = data;
        // First we need to compute the min and max for each feature.
        this.rangeMap = new HashMap<RnaData.Row, RangeNormalizer>(data.rows() * 4 / 3);
        for (RnaData.Row row : this.data) {
            RangeNormalizer rn = new RangeNormalizer();
            IntStream.range(0, row.size()).filter(i -> row.isGood(i))
                    .forEach(i -> rn.addElement(row.getWeight(i).getWeight()));
            this.rangeMap.put(row, rn);
        }
        // We run the correlations in parallel.  This requires synchronizing when a line is written,
        // which is why the write method is synchronized.
        IntStream.range(0, data.size()).parallel().forEach(i -> this.processSample(i));
    }

    /**
     * Process all the correlations for the sample at the specified index.
     *
     * @param data		RNA sample database
     */
    private void processSample(int jobIdx) {
        // Get the sample in question.
        RnaData.JobData sample1 = this.data.getJob(jobIdx);
        if (! this.pureMode || sample1.isGood()) {
            // Loop through the other samples.
            for (int i = jobIdx + 1; i < this.data.size(); i++) {
                RnaData.JobData sample2 = this.data.getJob(i);
                if (! this.pureMode || sample2.isGood()) {
                    // We will accumulate the errors and the count in here.
                    double errorSum = 0.0;
                    double count = 0;
                    for (RnaData.Row row : this.data) {
                        if (row.isGood(jobIdx) && row.isGood(i)) {
                            RnaData.Weight w1 = row.getWeight(jobIdx);
                            RnaData.Weight w2 = row.getWeight(i);
                            RangeNormalizer rn = this.rangeMap.get(row);
                            double err = rn.difference(w1.getWeight(), w2.getWeight());
                            errorSum += Math.abs(err);
                            count++;
                        }
                    }
                    if (count > 0) {
                        // Compute the mean absolute error and write the output.
                        double sim = 1.0 - (errorSum / count);
                        this.writeCorrelation(sample1, sample2, sim);
                    }
                }
            }
        }
    }

    /**
     * Write a line of output.  This method is synchronized, because the correlations are computed
     * in parallel: however, this means that the output is in more or less random order.
     *
     * @param sample1	first sample being correlated
     * @param sample2	second sample being correlated
     * @param corr		pearson coefficient of correlation
     */
    private synchronized void writeCorrelation(RnaData.JobData sample1, RnaData.JobData sample2, double corr) {
        this.formatln("%s\t%s\t%8.6f", sample1.getName(), sample2.getName(), corr);
        this.outputCount++;
        if (this.outputCount % 1000 == 0)
            log.info("{} correlations processed.", this.outputCount);
    }

}
