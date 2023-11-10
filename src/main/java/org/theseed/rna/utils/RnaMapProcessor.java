/**
 *
 */
package org.theseed.rna.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.cli.CopyTask;
import org.theseed.cli.DirEntry;
import org.theseed.cli.DirEntry.Type;
import org.theseed.cli.DirTask;
import org.theseed.io.LineReader;

/**
 * This command consolidates the RNA sequence expression data for multiple processing batches.  The positional parameters
 * are the names of the input directory, the output directory, and the PATRIC user name.  Both directories should be PATRIC
 * workspaces.
 *
 * Each sample will have a SAMSTAT report with a filename of the form
 *
 * 	Tuxedo_0_replicateN_XXXXXXXX_1_ptrim.fq_XXXXXXXX_2.ptrim.fq.bam.samstat.html
 *
 * where "N" is a number and "XXXXXXXX" is the sample ID.  The TPM mapping file corresponding to this will have the name
 *
 * 	Tuxedo_0_replicateN_genes.fpkm_tracking
 *
 * These files will be renamed to
 *
 * 	XXXXXXXX.samstat.html
 * 	XXXXXXXX_genes.fpkm
 *
 * respectively, in the output folder.
 *
 * PATRIC does not allow overwriting existing files, so samples already in the output folder will be skipped.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	show more detailed progress
 *
 * --workDir	temporary working directory; default is "Temp" in the current directory
 * --blackList	if specified, a file containing the job names for directories that should be skipped, one per line
 *
 * @author Bruce Parrello
 *
 */
public class RnaMapProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(RnaMapProcessor.class);
    /** set of samples already present in the output folder */
    private Set<String> outputSamples;
    /** map of replicate numbers to sample IDs */
    private Map<String, String> sampleMap;
    /** set of replicate numbers with TPM files */
    private Set<String> replicateSet;
    /** set of names for bad jobs to skip */
    private Set<String> badJobs;
    /** pattern for SAMSTAT html file name */
    private static final Pattern SAMSTAT_PATTERN = Pattern.compile("Tuxedo_0_replicate(\\d+)_([^_]+)_\\S+\\.bam\\.samstat\\.html");
    /** pattern for TPM file name */
    private static final Pattern TPM_PATTERN = Pattern.compile("Tuxedo_0_replicate(\\d+)_genes\\.fpkm_tracking");
    /** SAMSTAT output file name suffix */
    private static final String SAMSTAT_SUFFIX = ".samstat.html";
    /** TPM output file name suffix */
    private static final String TPM_SUFFIX = "_genes.fpkm";

    // COMMAND-LINE OPTIONS

    /** work directory for temporary files */
    @Option(name = "--workDir", metaVar = "workDir", usage = "work directory for temporary files")
    private File workDir;

    /** file containing names of jobs to skip */
    @Option(name = "--blackList", metaVar = "badJobs.tbl", usage = "file containing names of jobs to skip")
    private File blackListFile;

    /** input directory */
    @Argument(index = 0, metaVar = "inDir", usage = "input directory in PATRIC workspace", required = true)
    private String inDir;

    /** output directory */
    @Argument(index = 1, metaVar = "outDir", usage = "output directory in PATRIC workspace", required = true)
    private String outDir;

    /** workspace name */
    @Argument(index = 2, metaVar = "user@patricbrc.org", usage = "PATRIC workspace name", required = true)
    private String workspace;

    @Override
    protected void setDefaults() {
        this.workDir = new File(System.getProperty("user.dir"), "Temp");
        this.blackListFile = null;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        if (! this.workDir.isDirectory()) {
            log.info("Creating work directory {}.", this.workDir);
            FileUtils.forceMkdir(this.workDir);
            this.workDir.deleteOnExit();
        } else
            log.info("Temporary files will be stored in {}.", this.workDir);
        if (this.blackListFile == null)
            this.badJobs = Collections.emptySet();
        else {
            this.badJobs = LineReader.readSet(this.blackListFile);
            log.info("{} bad job names read from {},", this.badJobs.size(), this.blackListFile);
        }
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Scan the output directory to find the samples already copied.
        scanOutputDirectory();
        // The input directory contains multiple jobs.  We get the list of jobs and process them individually.
        List<String> jobs = scanMainDirectory();
        // Scan this input directory to find the replicate numbers and sample IDs.
        for (String job : jobs) {
            scanInputDirectory(job);
            // Create the copy helper.
            CopyTask copyTask = new CopyTask(this.workDir, this.workspace);
            // Loop through the TPM files.
            for (String replicateNum : this.replicateSet) {
                String sampleId = this.sampleMap.get(replicateNum);
                if (sampleId == null)
                    log.warn("No SAMSTAT file exists for replicate {}.", replicateNum);
                else if (this.outputSamples.contains(sampleId))
                    log.info("Skipping {}-- already copied.", sampleId);
                else {
                    log.info("Copying files for sample {}.", sampleId);
                    // Compute the input directory file names.
                    String samStatName = job + "/Tuxedo_0_replicate" + replicateNum + "_" + sampleId
                            + "_1_ptrim.fq_" + sampleId + "_2_ptrim.fq.bam.samstat.html";
                    String fpkmName = job + "/Tuxedo_0_replicate" + replicateNum + "_genes.fpkm_tracking";
                    // Copy them to the output.
                    copyTask.copyRemoteFile(samStatName, this.outDir + "/TPM/" + sampleId + SAMSTAT_SUFFIX);
                    copyTask.copyRemoteFile(fpkmName, this.outDir + "/TPM/" + sampleId + TPM_SUFFIX);
                    this.outputSamples.add(sampleId);
                }
            }
        }
    }

    /**
     * Scan the main input directory to find all the job directories in it.
     *
     * @return a list of job directory names
     */
    private List<String> scanMainDirectory() {
        // List the input directory.
        DirTask dirTask = new DirTask(this.workDir, this.workspace);
        log.info("Scanning main input directory {}.", this.inDir);
        List<DirEntry> inFiles = dirTask.list(this.inDir);
        // Create the output list.
        List<String> retVal = new ArrayList<String>(inFiles.size());
        for (DirEntry inFile : inFiles) {
            if (inFile.getType() == Type.JOB_RESULT) {
                String jobName = inFile.getName();
                if (this.badJobs.contains(jobName))
                    log.info("Skipping bad job {}.", jobName);
                else
                    retVal.add(this.inDir + "/." + jobName);
            }
        }
        log.info("{} job directories found in {}.", retVal.size(), this.inDir);
        return retVal;
    }

    /**
     * Analyze the input directory to find the replicate numbers and the associated sample IDs.
     *
     * @param jobDir	name of job directory
     */
    private void scanInputDirectory(String jobDir) {
        // List the job directory.
        DirTask dirTask = new DirTask(this.workDir, this.workspace);
        log.info("Scanning job directory {}.", jobDir);
        List<DirEntry> inFiles = dirTask.list(jobDir);
        // Run through the job directory files, creating the sample map.  We will also track the replicate numbers
        // for which TPM files exist.
        this.replicateSet = new HashSet<String>((inFiles.size() * 4 + 2) / 3);
        this.sampleMap = new HashMap<String, String>((inFiles.size() * 4 + 2) / 3);
        for (DirEntry inFile : inFiles) {
            Matcher m = SAMSTAT_PATTERN.matcher(inFile.getName());
            if (m.matches()) {
                // Here we have a SAMSTAT file.  Map the replicate number to the sample ID.
                this.sampleMap.put(m.group(1), m.group(2));
            } else {
                m = TPM_PATTERN.matcher(inFile.getName());
                if (m.matches()) {
                    // Here we have an TPM file.  Save the replicate number.
                    replicateSet.add(m.group(1));
                }
            }
        }
        log.info("{} SAMSTAT files found.  {} TPM files found.", this.sampleMap.size(), this.replicateSet.size());
    }

    /**
     * Scan the output directory for samples already copied.
     */
    private void scanOutputDirectory() {
        DirTask dirTask = new DirTask(this.workDir, this.workspace);
        // List all the files in the output directory.
        log.info("Scanning output directory {}.", this.outDir);
        List<DirEntry> outFiles = dirTask.list(this.outDir + "/TPM");
        // Save all the samples already copied.
        this.outputSamples = outFiles.stream().map(x -> x.getName()).filter(x -> x.endsWith(TPM_SUFFIX))
                .map(x -> StringUtils.removeEnd(x, TPM_SUFFIX)).collect(Collectors.toSet());
        log.info("{} samples found in output directory.", this.outputSamples.size());
    }

}
