/**
 *
 */
package org.theseed.reports;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.theseed.rna.RnaData;
import org.theseed.rna.RnaFeatureData;

/**
 * This is a simple report that displays the major distribution statistics for each feature.  It tells us
 * how much variance there is in the samples for each of the features.
 *
 * @author Bruce Parrello
 *
 */
public class BaselineSampleCheckReporter extends SampleCheckReporter {

    // FIELDS
    /** TRUE if only good samples should be processed */
    private boolean pureMode;

    /**
     * Construct a baseline report.
     *
     * @param processor		controlling command processor
     *
     * @throws IOException
     */
    public BaselineSampleCheckReporter(IParms processor) throws IOException {
        this.pureMode = processor.getPure();
    }

    @Override
    protected void writeHeader() {
        this.println("fid\tbaseline\tstd_dev\tmin\tmax\tskew\tcount\tCV\tskew_ratio\tgene_length\tfunction");
    }

    @Override
    public void generate(RnaData data) {
        log.info("{} features to process.", data.getRows().size());
        // We produce one report line for each feature.  A feature is represented by a data row.  The following
        // map sorts the output.
        var sortMap = new TreeMap<String, String>(new NaturalSort());
        // Now do the loop.
        int count = 0;
        for (RnaData.Row row : data) {
            DescriptiveStatistics stats = new DescriptiveStatistics();
            for (RnaData.Weight weight : data.new GoodWeights(row, pureMode))
                stats.addValue(weight.getWeight());
            RnaFeatureData feat = row.getFeat();
            // Before we leave, we need to calculate some derived statistics.
            double mean = stats.getMean();
            double sdev = stats.getStandardDeviation();
            double min = stats.getMin();
            double max = stats.getMax();
            double range = max - min;
            double cv = sdev / (mean - min);
            double skew_ratio = stats.getSkewness() / range;
            String fid = feat.getId();
            sortMap.put(fid, String.format("%s\t%8.2f\t%8.2f\t%8.2f\t%8.2f\t%8.2f\t%d\t%6.4f\t%6.4f\t%d\t%s",
                    fid, mean, sdev, min, max, stats.getSkewness(), stats.getN(), cv, skew_ratio,
                    feat.getLocation().getLength(), feat.getFunction()));
            count++;
            if (count % 100 == 0)
                log.info("{} features processed.", count);
        }
        log.info("{} features processed by report.", count);
        // Unspool the sorted features.
        sortMap.values().stream().forEach(x -> this.println(x));
    }

}
