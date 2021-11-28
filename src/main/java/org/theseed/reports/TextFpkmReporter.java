/**
 *
 */
package org.theseed.reports;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.theseed.rna.RnaData;
import org.theseed.rna.RnaFeatureData;

/**
 * This is a simple output class that produces the data in a simple tab-delimited file.
 *
 * @author Bruce Parrello
 *
 */
public class TextFpkmReporter extends FpkmReporter {

    // FIELDS
    /** buffer for building output lines */
    private StringBuffer buffer;
    /** number of actual samples */
    private int nSamples;
    /** print writer for output */
    private PrintWriter writer;
    /** quote string */
    private String quote;
    /** delimiter character */
    private String delim;
    /** format for weight columns */
    private String weightFormat;
    /** constant headings */
    private static final String[] HEADINGS = new String[] { "fid", "gene", "bNumber", "function", "neighbor", "AR_num", "iModulons", "baseLine" };

    /**
     * Construct this report.
     *
     * @param output		output stream
     * @param processor		controlling processor object
     */
    public TextFpkmReporter(OutputStream output, IParms processor, String delim, String quote) {
        this.writer = new PrintWriter(output);
        this.delim = delim;
        this.quote = quote;
        this.weightFormat = this.delim + "%8.4f";
    }

    @Override
    protected void openReport(List<RnaData.JobData> samples) {
        // Here we write the headings.
        String headings = StringUtils.join(HEADINGS, this.delim);
        this.writer.println(headings + this.delim
                + samples.stream().map(x -> x.getName()).collect(Collectors.joining(this.delim)));
        // Create the string buffer.
        this.nSamples = samples.size();
        this.buffer = new StringBuffer(100 + 15 * this.nSamples);
    }

    @Override
    protected void writeRow(RnaData.Row row) {
        // Here we write the row.  We need to get some data items out of the feature.
        RnaFeatureData feat = row.getFeat();
        String function = feat.getFunction();
        String gene = feat.getGene();
        String bNumber = feat.getBNumber();
        RnaFeatureData neighbor = row.getNeighbor();
        String neighborId = "";
        if (neighbor != null) {
            neighborId = neighbor.getId();
        }
        this.buffer.setLength(0);
        String[] fields = new String[] { feat.getId(), gene, bNumber, this.quote + function + this.quote,
                neighborId, Integer.toString(feat.getAtomicRegulon()), StringUtils.join(feat.getiModulons(), ','),
                String.format(this.weightFormat, feat.getBaseLine()) };
        this.buffer.append(StringUtils.join(fields, this.delim));
        for (int i = 0; i < this.nSamples; i++) {
            RnaData.Weight weight = row.getWeight(i);
            if (weight == null)
                this.buffer.append(this.delim);
            else {
                this.buffer.append(String.format(this.weightFormat, weight.getWeight()));
            }
        }
        this.writer.println(this.buffer.toString());
    }

    @Override
    protected void closeReport() {
        this.writer.close();
    }

    /**
     * Tab-delimited text report.
     */
    public static class Tab extends TextFpkmReporter {

        /**
         * Construct this report.
         *
         * @param output		output stream
         * @param processor		controlling processor object
         */
        public Tab(OutputStream output, IParms processor) {
            super(output, processor, "\t", "");
        }

    }

    /**
     * CSVtext report.
     */
    public static class CSV extends TextFpkmReporter {

        /**
         * Construct this report.
         *
         * @param output		output stream
         * @param processor		controlling processor object
         */
        public CSV(OutputStream output, IParms processor) {
            super(output, processor, ",", "\"");
        }

    }
}
