/**
 *
 */
package org.theseed.rna.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseReportProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.reports.SampleCheckReporter;
import org.theseed.reports.SampleCheckReporter.IParms;
import org.theseed.rna.RnaData;

/**
 * This command produces reports on the variability of the samples in an RNA seq sample database.
 * The command itself is very simple.  It merely loads the database and passes it to the
 * appropriate report method.
 *
 * The positional parameter is file name for the RNA seq expression database.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	output file for report (if not STDOUT)
 *
 * --format		report format
 * --pure		only include good samples
 * --clusters	the name of a file containing a tabular cluster report of the samples (WEIGHTED format)
 *
 * @author Bruce Parrello
 *
 */
public class SampleCheckProcessor extends BaseReportProcessor implements IParms {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SampleCheckProcessor.class);
    /** rna seq database */
    private RnaData data;
    /** report writer */
    private SampleCheckReporter reporter;

    // COMMAND-LINE OPTIONS

    /** format of the report */
    @Option(name = "--format", usage = "type of report to write")
    private SampleCheckReporter.Type reportType;

    /** if specified, only good samples will be included */
    @Option(name = "--pure", usage = "if specified, only good samples will be processed")
    private boolean pureMode;

    /** the name of the cluster file for the WEIGHTED report */
    @Option(name = "--clusters", metaVar = "clusters.tbl", usage = "file containing tabular cluster report (WEIGHTED only)")
    private File clusterFile;

    /** RNA seq expression database file name */
    @Argument(index = 0, metaVar = "rnaData.ser", usage = "input RNA seq expression database")
    private File rnaFile;

    @Override
    protected void setReporterDefaults() {
        this.reportType = SampleCheckReporter.Type.BASELINE;
        this.pureMode = false;
        this.clusterFile = null;
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        // Validate the RNA database.
        if (! this.rnaFile.canRead())
            throw new FileNotFoundException("RNA data file " + this.rnaFile + " is not found or unreadable.");
        try {
            log.info("Loading RNA data from {}.", this.rnaFile);
            this.data = RnaData.load(this.rnaFile);
        } catch (ClassNotFoundException e) {
            throw new IOException("Error in RNA data file: " + e.toString());
        }
        // Create the report object.
        this.reporter = this.reportType.create(this);

    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        log.info("Producing report.");
        this.reporter.openReport(writer);
        this.reporter.generate(data);
    }

    @Override
    public boolean getPure() {
        return this.pureMode;
    }

    @Override
    public File getClusterFile() {
        return this.clusterFile;
    }

}
