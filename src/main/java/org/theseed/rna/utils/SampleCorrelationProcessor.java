/**
 *
 */
package org.theseed.rna.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.erdb.utils.BaseDbReportProcessor;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.Relop;
import org.theseed.reports.RangeNormalizer;
import org.theseed.utils.ParseFailureException;

/**
 * This command reads a genome's RNA samples from the database and outputs a correlation report.
 * The report can then be used to cluster the samples into groups.
 *
 * The positional parameter is the ID of the genome whose samples should be processed.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	output file (if not STDOUT)
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 * --all		if specified, suspicious samples will be included
 *
 * @author Bruce Parrello
 *
 */
public class SampleCorrelationProcessor extends BaseDbReportProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SampleCorrelationProcessor.class);
    /** array of range normalizers for the features */
    private RangeNormalizer[] fidRanges;
    /** map of sample IDs to expression level arrays */
    private List<Map.Entry<String, double[]>> sampleLevels;
    /** number of comparisons made */
    private int compareCount;
    /** total number of comparisons required */
    private int totalCount;

    // COMMAND-LINE OPTIONS

    /** TRUE if suspicious samples should be included */
    @Option(name = "--all", usage = "if specified, suspicious samples will be included in the clusters")
    private boolean allFlag;

    /** ID of the genome of interest */
    @Argument(index = 0, metaVar = "genomeId", usage = "ID of the genome whose samples are to be clustered",
            required = true)
    private String genomeId;

    @Override
    protected void setReporterDefaults() {
        this.allFlag = false;
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
    }

    @Override
    protected void runDbReporter(DbConnection db, PrintWriter writer) throws Exception {
        // First, we get the genome itself.  This verifies that it exists, and nets us the
        // array size for the expression levels.
        DbRecord genomeRecord = db.getRecord("Genome", this.genomeId);
        if (genomeRecord == null)
            throw new ParseFailureException("Invalid reference genome ID \"" + this.genomeId + "\": not found.");
        // Get the feature count.
        final int nPegs = genomeRecord.getInt("Genome.peg_count");
        String gName = genomeRecord.getString("Genome.genome_name");
        log.info("Genome {} ({}) contains {} pegs.", genomeId, gName, nPegs);
        // Create the normalizer array.
        this.fidRanges = IntStream.range(0, nPegs).mapToObj(i -> new RangeNormalizer()).toArray(RangeNormalizer[]::new);
        this.sampleLevels = new ArrayList<Map.Entry<String, double[]>>(1000);
        // We start by querying the database to get the sample data.  The basic query is for
        // all samples for the genome.  If the all-flag is FALSE, we restrict to samples that
        // are not suspicious.
        log.info("Reading samples from database.");
        try (DbQuery query = new DbQuery(db, "RnaSample")) {
            query.select("RnaSample", "sample_id", "feat_data");
            query.rel("RnaSample.genome_id", Relop.EQ);
            query.setParm(1, this.genomeId);
            if (! this.allFlag) {
                query.rel("RnaSample.suspicious", Relop.EQ);
                query.setParm(2, false);
            }
            // We loop through the samples.  For each one, we stash its levels array in the sample map, and
            // then update the normalizers.
            for (DbRecord sample : query) {
                double[] levels = sample.getDoubleArray("RnaSample.feat_data");
                String sampleId = sample.getString("RnaSample.sample_id");
                for (int i = 0; i < nPegs; i++) {
                    if (Double.isFinite(levels[i]))
                        this.fidRanges[i].addElement(levels[i]);
                }
                this.sampleLevels.add(new AbstractMap.SimpleImmutableEntry<>(sampleId, levels));
            }
        }
        // Now we have everything we need in memory.  Write the output header.
        writer.println("sample1\tsample2\tsimilarity");
        // Compute the total number of comparisons required so we can count.
        int nSamples = this.sampleLevels.size();
        this.totalCount = nSamples * (nSamples + 1) / 2;
        this.compareCount = 0;
        // Loop through the samples.  For each sample, we compute its correlations.
        IntStream.range(0, nSamples).parallel().forEach(i ->
                this.processSample(writer, this.sampleLevels.get(i), this.sampleLevels.subList(i+1, nSamples)));
    }

    /**
     * This method computes the correlation for a single sample.  The incoming entry is compared to
     * every entry in the sublist, which is to say, every subsequent sample.  This method is
     * executed in parallel, so the subroutine that writes to the output is synchronized.
     *
     * @param writer	output writer for the results
     * @param entry		main sample entry
     * @param subList	list of other sample entries to which it should be compared
     */
    private void processSample(PrintWriter writer, Entry<String, double[]> entry, List<Map.Entry<String, double[]>> subList) {
        String sample1 = entry.getKey();
        double[] levels1 = entry.getValue();
        // Loop through the other samples.
        for (Map.Entry<String, double[]> entry2 : subList) {
            String sample2 = entry2.getKey();
            double[] levels2 = entry2.getValue();
            // Now we accumulate the error sum and the total count.  The maximum error at each position is 1.0
            // because of the normalizing.  A position is only counted if it is finite in both arrays.
            double errorSum = 0.0;
            int count = 0;
            for (int i = 0; i < levels1.length; i++) {
                double v1 = levels1[i];
                double v2 = levels2[i];
                if (Double.isFinite(v1) && Double.isFinite(v2)) {
                    double err = this.fidRanges[i].difference(v1, v2);
                    errorSum += Math.abs(err);
                    count++;
                }
            }
            // The similarity is 1 - mean-absolute-error.
            double sim = (count > 0 ? 1.0 - (errorSum / count) : 0.0);
            this.writeCorrelation(writer, sample1, sample2, sim);
        }
    }

    /**
     * Write out the correlation results and document our progress.
     *
     * @param sample1		name of first sample
     * @param sample2		name of second sample
     * @param sim			similarity score
     */
    private void writeCorrelation(PrintWriter writer, String sample1, String sample2, double sim) {
        writer.format("%s\t%s\t%8.6f%n", sample1, sample2, sim);
        this.compareCount++;
        if (this.compareCount % 5000 == 0)
            log.info("{} of {} comparisons computed.", this.compareCount, this.totalCount);
    }

}
