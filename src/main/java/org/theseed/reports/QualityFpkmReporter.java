/**
 *
 */
package org.theseed.reports;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;

import org.theseed.rna.RnaData.JobData;
import org.theseed.rna.RnaData.Row;

/**
 * This report displays the quality-related data for each sample.  The actual TPM levels are not displayed.
 *
 * @author Bruce Parrello
 *
 */
public class QualityFpkmReporter extends FpkmReporter {

    // FIELDS
    /** output writer */
    private PrintWriter writer;
    /** dna size of the base genome */
    private int genomeSize;

    /**
     * Construct a reporter for sample quality information.
     *
     * @param output		output stream
     * @param processor		controlling command processor
     */
    public QualityFpkmReporter(OutputStream output, IParms processor) {
        this.writer = new PrintWriter(output);
        this.genomeSize = processor.getGenomeLen();
    }

    @Override
    protected void openReport(List<JobData> list) {
        // Here we output the actual report.  The quality data is in the JobData records.
        this.writer.println("name\tsuspicious\tpct_quality\treads\tbases\tcoverage\tpct_expressed");
        for (JobData job : list) {
            long bases = job.getBaseCount();
            double quality = job.getQuality();
            double coverage = (bases * quality) / (this.genomeSize * 100.0);
            writer.format("%s\t%s\t%8.4f\t%d\t%d\t%8.4f\t%8.4f%n", job.getName(), (job.isSuspicious() ? "Y" : ""),
                    quality, job.getReadCount(), bases, coverage, job.getExpressedPercent());
        }
    }

    @Override
    protected void writeRow(Row row) {
    }

    @Override
    protected void closeReport() {
        this.writer.close();
    }

}
