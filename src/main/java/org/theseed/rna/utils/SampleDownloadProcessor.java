/**
 *
 */
package org.theseed.rna.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.cli.CopyTask;
import org.theseed.rna.jobs.RnaJob;
import org.theseed.utils.BaseReportProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This command downloads all the samples from a PATRIC RNA-seq processing directory and produces a metadata file for them.
 * This is designed specifically for NCBI samples, so that they can be efficiently processed by FpmkSummaryProcessor.
 *
 * The positional parameters are the name of the PATRIC directory (generally ending in "/FPKM"), the PATRIC workspace ID, and
 * the name of the local output directory.  The samples will be downloaded into a subdirectory named FPKM.  The output file will
 * be produced on the standard output.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	output file for the sample list (if not STDOUT)
 *
 * --workDir	working directory for temporary files (default is "Temp" in the current directory
 *
 * @author Bruce Parrello
 *
 */
public class SampleDownloadProcessor extends BaseReportProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SampleDownloadProcessor.class);
    /** output directory for copied files */
    private File fpkmDir;

    // COMMAND-LINE OPTIONS

    /** work directory for temporary files */
    @Option(name = "--workDir", metaVar = "workDir", usage = "work directory for temporary files")
    private File workDir;

    /** PATRIC directory containing the samples */
    @Argument(index = 0, metaVar = "user@patricbrc.org/inputDirectory", usage = "PATRIC input directory for samples", required = true)
    private String inDir;

    /** controlling workspace name */
    @Argument(index = 1, metaVar = "user@patricbrc.org", usage = "controlling workspace", required = true)
    private String workspace;

    /** output directory name */
    @Argument(index = 2, metaVar = "sampleDir", usage = "output directory name", required = true)
    private File outDir;

    @Override
    protected void setReporterDefaults() {
        this.workDir = new File(System.getProperty("user.dir"), "Temp");
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        // Verify we have a work directory.
        if (! this.workDir.isDirectory()) {
            log.info("Creating work directory {}.", this.workDir);
            FileUtils.forceMkdir(this.workDir);
        } else
            log.info("Working directory is {}.", this.workDir);
        // Make sure the PATRIC directory is absolute.
        if (! StringUtils.startsWith(this.inDir, "/"))
            throw new ParseFailureException("PATRIC input directory must be absolute.");
        // Insure the output directory exists.
        if (! this.outDir.isDirectory()) {
            log.info("Creating output directory {}.", this.outDir);
            FileUtils.forceMkdir(this.outDir);
        }
        // Prepare the FPKM subdirectory.  If it exists, it must be deleted or the copy task will fail.
        fpkmDir = new File(this.outDir, RnaJob.FPKM_DIR);
        if (fpkmDir.exists())
            FileUtils.forceDelete(fpkmDir);
        log.info("Output will be to {}.", fpkmDir);
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        // Write the output header.
        writer.println("sample\tthr_mg/l\tOD_600m\told_name\tsuspicious");
        // Copy the files from PATRIC to our output directory.
        log.info("Copying FPKM tracking files from {}.", this.inDir);
        CopyTask copy = new CopyTask(this.workDir, this.workspace);
        File[] fpkmFiles = copy.copyRemoteFolder(this.inDir + "/" + RnaJob.FPKM_DIR, true);
        log.info("{} files copied into {}.", fpkmFiles.length, fpkmDir);
        // Now write the file names to the output file.
        int count = 0;
        for (File fpkmFile : fpkmFiles) {
            String jobName = RnaJob.Phase.COPY.checkSuffix(fpkmFile.getName());
            if (jobName != null) {
                writer.println(jobName + "\t\t\t\t");
                count++;
            }
        }
        log.info("{} samples found.", count);
    }

}
