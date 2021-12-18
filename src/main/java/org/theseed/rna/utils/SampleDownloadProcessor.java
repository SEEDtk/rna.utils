/**
 *
 */
package org.theseed.rna.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.cli.CopyTask;
import org.theseed.cli.DirTask;
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
        // Prepare the FPKM subdirectory.
        fpkmDir = new File(this.outDir, RnaJob.FPKM_DIR);
        if (! fpkmDir.isDirectory()) {
            log.info("Creating output FPKM subdirectory.");
            FileUtils.forceMkdir(fpkmDir);
        }
        log.info("Samples will be copied to {}.", fpkmDir);
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        // Write the output header.
        writer.println("sample\tthr_mg/l\tOD_600m\told_name\tsuspicious");
        // Get the files already in the output directory.
        Set<String> processed = Arrays.stream(this.fpkmDir.list()).collect(Collectors.toCollection(TreeSet::new));
        log.info("{} files already found in output directory.", processed.size());
        // Get all the files in the input directory, eliminating the ones already processed.
        String remoteFolder = (this.inDir + "/" + RnaJob.FPKM_DIR);
        DirTask lister = new DirTask(this.workDir, this.workspace);
        Set<String> files = lister.list(remoteFolder).stream().map(x -> x.getName())
                .filter(x -> ! processed.contains(x)).collect(Collectors.toSet());
        // Now we have all the files to copy.
        log.info("{} new files found in PATRIC directory {}.", files.size(), this.inDir);
        CopyTask copy = new CopyTask(this.outDir, this.workspace);
        // If there were no files found in the directory, try a full directory copy.
        if (processed.size() == 0) {
            log.info("Performing full-directory copy.");
            File[] newFiles = copy.copyRemoteFolder(remoteFolder, true);
            log.info("{} files copied.", newFiles.length);
            Arrays.stream(newFiles).forEach(x -> processed.add(x.getName()));
        } else {
            // Loop through the files in the remote folder
            log.info("Performing file-by-file copy.");
            long start = System.currentTimeMillis();
            int copied = 0;
            int total = files.size();
            for (String remoteName : files) {
                String remoteFile = remoteFolder + "/" + remoteName;
                File localFile = new File(this.fpkmDir, remoteName);
                copy.copyRemoteFile(remoteFile, localFile);
                processed.add(remoteName);
                if (log.isInfoEnabled()) {
                    copied++;
                    if (copied % 10 == 0) {
                        long timeLeft = (System.currentTimeMillis() - start) * (total - copied) / (copied * 1000);
                        log.info("{} files copied.  {} seconds remaining.", copied, timeLeft);
                    }
                }
            }
        }
        log.info("{} total files now in {}.", processed.size(), this.fpkmDir);
        // Now write the file names to the output file.
        int count = 0;
        for (String fileName : processed) {
            String jobName = RnaJob.Phase.COPY.checkSuffix(fileName);
            if (jobName != null) {
                writer.println(jobName + "\t\t\t\t");
                count++;
            }
        }
        log.info("{} samples found.", count);
    }

}
