/**
 *
 */
package org.theseed.rna.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.cli.DirEntry;
import org.theseed.cli.DirTask;
import org.theseed.utils.BaseReportProcessor;

/**
 * This command analyzes the RNA sequencing jobs in a PATRIC directory and produces a report on how many failed and
 * how many worked.
 *
 * The positional parameters are the name of the input directory and the relevant PATRIC workspace name.  The command-line
 * options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent error messages
 * -o	output file for report (if not STDOUT)
 *
 * --workDir	temporary working directory (default C<Temp> in the current directory)
 *
 * @author Bruce Parrello
 *
 */
public class AuditProcessor extends BaseReportProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(AuditProcessor.class);
    /** directory-listing task */
    private DirTask listDir;
    /** number of good jobs found */
    private int goodCount;
    /** number of unpaired jobs found */
    private int unpairedCount;
    /** set of failed jobs found */
    private Set<String> badJobs;

    // COMMAND-LINE OPTIONS

    /** work directory for temporary files */
    @Option(name = "--workDir", metaVar = "workDir", usage = "work directory for temporary files")
    private File workDir;

    /** input directory */
    @Argument(index = 0, metaVar = "inDir", usage = "input directory in PATRIC workspace", required = true)
    private String inDir;

    /** workspace name */
    @Argument(index = 1, metaVar = "user@patricbrc.org", usage = "PATRIC workspace name", required = true)
    private String workspace;

    @Override
    protected void setReporterDefaults() {
        this.workDir = new File(System.getProperty("user.dir"), "Temp");
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        // Insure we have a work directory.
        if (! this.workDir.isDirectory()) {
            log.info("Creating work directory {}.", this.workDir);
            FileUtils.forceMkdir(this.workDir);
        } else
            log.info("Working directory is {}.", this.workDir);
        // Create the directory-list task.
        this.listDir = new DirTask(this.workDir, this.workspace);
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        // Get the jobs in the input directory.
        log.info("Retrieving job list from {}.", this.inDir);
        List<DirEntry> inEntries = this.listDir.list(this.inDir).stream().filter(x -> x.getType() == DirEntry.Type.JOB_RESULT)
                .collect(Collectors.toList());
        log.info("{} jobs found in input directory.", inEntries.size());
        // On our first pass, we process the RNA jobs and accumulate a set of the jobs that failed.  A successful
        // RNA job is simply counted.
        this.goodCount = 0;
        this.badJobs = new HashSet<String>(inEntries.size());
        inEntries.parallelStream().filter(x -> x.getName().endsWith("_rna")).forEach(x -> this.checkRnaJob(x));
        log.info("{} good jobs, {} bad jobs.", this.goodCount, this.badJobs.size());
        // Determine how many jobs failed because of not being paired-end.
        this.unpairedCount = 0;
        this.badJobs.parallelStream().forEach(x -> this.checkFastqJob(x));
        log.info("{} unpaired jobs.", this.unpairedCount);
        // Write the counts.
        int lowJobs = this.badJobs.size() - this.unpairedCount;
        writer.println("Status\tcount");
        writer.format("%s\t%d%n", "Successful", this.goodCount);
        writer.format("%s\t%d%n", "Unpaired", this.unpairedCount);
        writer.format("%s\t%d%n", "Failed", lowJobs);
    }

    /**
     * Check a FASTQ job to see if it was paired-end.
     *
     * @param jobName	name of job to check
     */
    private void checkFastqJob(String jobName) {
        String jobDir = this.inDir + "/." + jobName;
        long qCount = this.listDir.list(jobDir).stream().filter(x -> x.getType() == DirEntry.Type.READS)
                .count();
        if (qCount < 2)
            synchronized(this) {
                this.unpairedCount++;
                log.info("{} is unpaired.", jobName);
            }
    }

    /**
     * Check an RNA job to see if it failed.
     *
     * @param jobResult		directory entry for the job result
     */
    private void checkRnaJob(DirEntry jobResult) {
        String jobName = jobResult.getName();
        String jobDir = this.inDir + "/." + jobName;
        long fCount = this.listDir.list(jobDir).stream().filter(x -> x.getName().startsWith("JobFailed"))
                .count();
        synchronized(this) {
            if (fCount == 0) {
                this.goodCount++;
                log.info("{} is good.", jobName);
            } else
                this.badJobs.add(jobName);
        }
    }

}
