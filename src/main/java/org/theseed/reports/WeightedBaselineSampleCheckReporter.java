/**
 *
 */
package org.theseed.reports;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;
import org.theseed.io.TabbedLineReader;
import org.theseed.rna.RnaData;
import org.theseed.rna.RnaFeatureData;
import org.theseed.utils.ParseFailureException;

/**
 * This report does baseline computation on clustered samples.  The baseline is computed independently
 * for each nontrivial cluster, and then a mean for all the clusters (including the trivial ones) is
 * output.  The result is written in matrix form.
 *
 * The clusters are read from a tabular cluster report.  (dl4j.clusters command "cluster", format TABULAR)
 *
 * @author Bruce Parrello
 *
 */
public class WeightedBaselineSampleCheckReporter extends SampleCheckReporter {

    // FIELDS
    /** map of cluster IDs to sample IDs */
    private Map<String, Set<String>> clusterMap;
    /** list of cluster names in order */
    private List<String> clusterNames;
    /** output buffer for each report line */
    private TextStringBuilder buffer;
    /** natural-order sorter for cluster names */
    private static final Comparator<String> NATURAL_SORTER = new NaturalSort();

    /**
     * Construct a weighted baseline sample-check report.
     *
     * @param processor controlling command processor
     *
     * @throws IOException
     * @throws ParseFailureException
     */
    public WeightedBaselineSampleCheckReporter(IParms processor) throws IOException, ParseFailureException {
        // Get the tabular cluster report.
        File clusterFile = processor.getClusterFile();
        if (clusterFile == null)
            throw new ParseFailureException("Cluster file required for WEIGHTED report.");
        // Create the count and cluster maps.  For each sample we need its cluster, and for each
        // cluster we need its count.
        this.clusterMap = new HashMap<String, Set<String>>(100);
        int sampleCount = 0;
        try (TabbedLineReader clusterStream = new TabbedLineReader(clusterFile)) {
            for (TabbedLineReader.Line line : clusterStream) {
                String cluster = line.get(0);
                String sample = line.get(1);
                Set<String> samples = this.clusterMap.computeIfAbsent(cluster, k -> new TreeSet<String>());
                samples.add(sample);
                sampleCount++;
            }
            log.info("{} samples found in {} clusters.", sampleCount, this.clusterMap.size());
        }
        // Get the list of cluster names in count order.  These will be our column headers.
        this.clusterNames = new ArrayList<String>(this.clusterMap.keySet());
        Collections.sort(this.clusterNames, this.new Sorter());
    }

    /**
     * This is a comparator class for sorting cluster names by size.
     */
    private class Sorter implements Comparator<String> {

        @Override
        public int compare(String o1, String o2) {
            int o1Count = WeightedBaselineSampleCheckReporter.this.clusterMap.get(o1).size();
            int o2Count = WeightedBaselineSampleCheckReporter.this.clusterMap.get(o2).size();
            int retVal = o2Count - o1Count;
            if (retVal == 0)
                retVal = NATURAL_SORTER.compare(o1, o2);
            return retVal;
        }

    }

    @Override
    protected void writeHeader() {
        this.println("fid\tgene\tbaseline\t" + StringUtils.join(this.clusterNames, '\t') + "\tlen\tfunction");
        this.buffer = new TextStringBuilder(80 + this.clusterNames.size() * 10);
    }

    @Override
    public void generate(RnaData data) {
        // For each cluster, we need a list of the job indices in that cluster.  This will map each cluster
        // name to an array of job indices.
        var clusterJobMap = new HashMap<String, int[]>(this.clusterNames.size() * 4 / 3);
        for (Map.Entry<String, Set<String>> clusterEntry : this.clusterMap.entrySet()) {
            String cluster = clusterEntry.getKey();
            int[] jobs = clusterEntry.getValue().stream().mapToInt(x -> data.getColIdx(x)).toArray();
            clusterJobMap.put(cluster, jobs);
        }
        // We will use this array to hold the average for each cluster.
        double[] baselines = new double[this.clusterNames.size()];
        // We process the features one at a time.  For each one, we compute the values for the various
        // clusters and then average them together to get the baseline.  We want, however, to process
        // them in location order, so we do a sort first.
        List<RnaData.Row> rows = data.getRows().stream().sorted().collect(Collectors.toList());
        for (RnaData.Row row : rows) {
            for (int i = 0; i < this.clusterNames.size(); i++) {
                double sum = 0.0;
                double count = 0;
                int[] jobList = clusterJobMap.get(this.clusterNames.get(i));
                for (int jobIdx : jobList) {
                    if (row.isGood(jobIdx)) {
                        RnaData.Weight weight = row.getWeight(jobIdx);
                        sum += weight.getWeight();
                        count++;
                    }
                }
                if (count == 0.0)
                    baselines[i] = 0.0;
                else
                    baselines[i] = sum / count;
            }
            // Now we have the baseline values for all of the clusters.  Next we need the mean baseline
            // and the gene length.
            double baseline = Arrays.stream(baselines).sum() / baselines.length;
            RnaFeatureData feat = row.getFeat();
            int geneLength = feat.getLocation().getLength();
            // Finally, we build the output line.
            this.buffer.clear();
            this.buffer.append(feat.getId()).append('\t').append(feat.getGene()).append('\t').append(baseline);
            Arrays.stream(baselines).forEach(v -> this.buffer.append('\t').append(v));
            this.buffer.append('\t').append(geneLength).append('\t').append(feat.getFunction());
            // Write the line.
            this.println(this.buffer.toString());
        }
    }

}
