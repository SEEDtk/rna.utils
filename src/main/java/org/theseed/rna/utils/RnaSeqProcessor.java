/**
 *
 */
package org.theseed.rna.utils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.cli.DirEntry;
import org.theseed.cli.DirTask;
import org.theseed.cli.MkDirTask;
import org.theseed.cli.StatusTask;
import org.theseed.counters.CountMap;
import org.theseed.p3api.P3Connection;
import org.theseed.rna.jobs.RnaJob;
import org.theseed.rna.jobs.RnaSeqGroup;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ParseFailureException;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This command runs a directory of RNA sequence data.  This is a complicated process that involves multiple steps.  First, the
 * reads must be trimmed.  Next, the trimmed reads must be aligned to the base genome.  Finally, the FPKM files must be copied
 * into the main output directory.
 *
 * The positional parameters are the ID of the base genome, the name of the input source
 * containing the fastq files, the name of the output directory, and the name of the workspace.
 * The directory names are PATRIC directories, not files in the file system.
 *
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	show more detailed progress
 * -p	pattern for FASTQ files; default is "(.+)_(R[12])_001\\.fastq"; used only by PATRIC_DIR sources
 * -l	identifier for left-read FASTQ files; default is "R1"; used only by PATRIC_DIR sources
 *
 * --maxIter	maximum number of loop iterations
 * --wait		number of minutes to wait
 * --workDir	temporary working directory; default is "Temp" in the current directory
 * --limit		limit when querying the PATRIC task list (must be large enough to capture all of the running tasks); default 1000
 * --maxTasks	maximum number of tasks to run at one time
 * --source		type of RNA Seq source; the default is PATRIC_DIR
 * --retries	maximum number of retries per job
 *
 * @author Bruce Parrello
 *
 */
public class RnaSeqProcessor extends BaseProcessor implements RnaSeqGroup.IParms {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(RnaSeqProcessor.class);
    /** map of currently-active jobs by name */
    private Map<String, RnaJob> activeJobs;
    /** task for getting task status */
    private StatusTask statusTask;
    /** task for reading a directory */
    private DirTask dirTask;
    /** update counter for job results */
    private int updateCounter;
    /** wait interval */
    private int waitInterval;
    /** RNA Seq source group */
    private RnaSeqGroup sourceGroup;
    /** job retry counter */
    private CountMap<String> retryCounts;

    // COMMAND-LINE OPTIONS

    /** pattern for left FASTQ files */
    @Option(name = "-p", aliases = { "--pattern" }, metaVar = "(.+)_(R[12])\\.fastq", usage = "regular expression for FASTQ file names")
    private String readRegex;

    /** identifier for left-read FASTQ files */
    @Option(name = "-l", aliases = { "--leftId" }, metaVar = "001", usage = "identifier for left-read FASTQ file names")
    private String leftId;

    /** maximum number of iterations to run */
    @Option(name = "--maxIter", metaVar = "70", usage = "maximum number of loop iterations to run before quitting (-1 to loop forever)")
    private int maxIter;

    /** number of minutes to wait between iterations */
    @Option(name = "--wait", metaVar = "5", usage = "number of minutes to wait between loop iterations")
    private int wait;

    /** work directory for temporary files */
    @Option(name = "--workDir", metaVar = "workDir", usage = "work directory for temporary files")
    private File workDir;

    /** maximum number of tasks to return when doing a task search */
    @Option(name = "--limit", metaVar = "100", usage = "maximum number of tasks to return during a task-progress query")
    private int limit;

    /** maximum number of tasks to run at one time */
    @Option(name = "--maxTasks", metaVar = "20", usage = "maximum number of running tasks to allow")
    private int maxTasks;

    /** type of RNA Seq source */
    @Option(name = "--source", usage = "RNA Seq source type")
    private RnaSeqGroup.Type sourceType;

    /** maximum number of retries per job */
    @Option(name = "--retries", metaVar = "5", usage = "maximum retries per job")
    private int maxRetries;

    /** base genome definition file */
    @Argument(index = 0, metaVar = "genome_id", usage = "ID of the base genome for this process", required = true)
    private String baseGenome;

    /** input directory */
    @Argument(index = 1, metaVar = "inDir", usage = "input directory in PATRIC workspace", required = true)
    private String inDir;

    /** output directory */
    @Argument(index = 2, metaVar = "outDir", usage = "output directory in PATRIC workspace", required = true)
    private String outDir;

    /** workspace name */
    @Argument(index = 3, metaVar = "user@patricbrc.org", usage = "PATRIC workspace name", required = true)
    private String workspace;

    @Override
    protected void setDefaults() {
        this.readRegex = "(.+)_(R[12])_001\\.fastq";
        this.leftId = "R1";
        this.workDir = new File(System.getProperty("user.dir"), "Temp");
        this.limit = 1000;
        this.wait = 7;
        this.maxIter = 100;
        this.maxTasks = 10;
        this.maxRetries = 3;
        this.sourceType = RnaSeqGroup.Type.PATRIC_DIR;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        if (! this.workDir.isDirectory()) {
            log.info("Creating work directory {}.", this.workDir);
            FileUtils.forceMkdir(this.workDir);
            this.workDir.deleteOnExit();
        } else
            log.info("Temporary files will be stored in {}.", this.workDir);
        // Insure the task limit is reasonable.
        if (this.limit < 100)
            throw new ParseFailureException("Invalid task limit.  The number should be enough to find all running tasks, and must be >= 100.");
        // Set up the library source.
        this.sourceGroup = this.sourceType.create(this);
        // Verify the base genome ID.
        P3Connection p3 = new P3Connection();
        JsonObject genomeRecord = p3.getRecord(P3Connection.Table.GENOME, this.baseGenome, "genome_id,genome_name");
        if (genomeRecord == null)
            throw new ParseFailureException("Genome \"" + this.baseGenome + "\" does not exist.");
        else
            log.info("Base genome is {}: {}.", this.baseGenome, genomeRecord.getOrDefault("genome_name", "unknown"));
        // Connect our parameters to the source group.
        this.sourceGroup.setGenomeId(this.baseGenome);
        log.info("PATRIC output directory is {}.", this.outDir);
        this.sourceGroup.setOutDir(this.outDir);
        // Verify the wait interval.
        if (this.wait < 0)
            throw new ParseFailureException("Invalid wait interval " + Integer.toString(this.wait) + ". Must be >= 0.");
        // Convert the interval from minutes to milliseconds.
        this.waitInterval = wait * (60 * 1000);
        // Verify the task limit.
        if (this.maxTasks < 1)
            throw new ParseFailureException("Maximum number of tasks must be > 0.");
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Initialize the retry counter.
        this.retryCounts = new CountMap<String>();
        // Initialize the utility tasks.
        this.dirTask = new DirTask(this.workDir, this.workspace);
        this.statusTask = new StatusTask(this.limit, this.workDir, this.workspace);
        // Get all the input RNA seq sources.
        this.activeJobs = this.sourceGroup.getJobs(this.inDir);
        // Determine the completed jobs from the output directory.
        this.scanOutputDirectory();
        // Now we have collected all the jobs.  Delete the ones that are incomplete.
        log.info("{} jobs found in input directory.", this.activeJobs.size());
        Iterator<Map.Entry<String, RnaJob>> iter = this.activeJobs.entrySet().iterator();
        while (iter.hasNext()) {
            RnaJob job = iter.next().getValue();
            if (! job.isPrepared())
                iter.remove();
        }
        log.info("{} jobs remaining after incomplete jobs removed.", this.activeJobs.size());
        // Now we have a list of active jobs and their current state.  Check for existing jobs.
        this.checkRunningJobs();
        // Loop until all jobs are done or we exceed the iteration limit.
        int remaining = this.maxIter;
        Set<RnaJob> incomplete = this.getIncomplete();
        while (! incomplete.isEmpty() && remaining != 0) {
            log.info("{} jobs in progress.", incomplete.size());
            this.processJobs(incomplete);
            incomplete = this.getIncomplete();
            remaining--;
            log.info("Sleeping. {} cycles left.", remaining);
            Thread.sleep(this.waitInterval);
        }
    }

    /**
     * Process all the currently-incomplete jobs.
     *
     * @param incomplete	set of currently-incomplete jobs
     */
    private void processJobs(Set<RnaJob> incomplete) {
        // Separate out the running tasks.
        log.info("Scanning job list.");
        Map<String, RnaJob> activeTasks = new HashMap<String, RnaJob>((incomplete.size() * 4 + 2) / 3);
        Set<RnaJob> needsTask = new TreeSet<RnaJob>();
        for (RnaJob job : incomplete) {
            String taskId = job.getTaskId();
            if (taskId == null)
                needsTask.add(job);
            else
                activeTasks.put(taskId, job);
        }
        // Check the state of the running tasks.
        Map<String, String> taskMap = this.statusTask.getStatus(activeTasks.keySet());
        for (Map.Entry<String, String> taskEntry : taskMap.entrySet()) {
            // Get the task state and the associated job.
            String state = taskEntry.getValue();
            String taskId = taskEntry.getKey();
            RnaJob job = activeTasks.get(taskId);
            String jobName = job.getName();
            switch (state) {
            case StatusTask.FAILED :
                // Task failed.  Restart it if we haven't run out of retries.
                int retries = this.retryCounts.count(jobName);
                if (retries >= this.maxRetries) {
                    log.error("Job {} failed in phase {} after {} retries.", jobName, job.getPhase(),
                            retries);
                    activeTasks.remove(taskId);
                    job.setFailed();
                } else {
                    log.warn("Job {} failed in phase {}.  Retrying.", jobName, job.getPhase());
                    job.startTask(this.workDir, this.workspace);
                }
                break;
            case StatusTask.COMPLETED :
                // Task completed.  Move to the next phase.
                log.info("Job {} completed phase {}.", jobName, job.getPhase());
                boolean done = job.nextPhase();
                // The job is no longer active.
                activeTasks.remove(taskId);
                // If it is not done, denote it needs a task.
                if (! done)
                    needsTask.add(job);
                break;
            default :
                // Here the job is still in progress.
                log.debug("Job {} still executing phase {}.", jobName, job.getPhase());
            }
        }
        int remaining = this.maxTasks - activeTasks.size();
        // Advance the non-running tasks.
        Iterator<RnaJob> iter = needsTask.iterator();
        while (iter.hasNext() && remaining > 0) {
            RnaJob job = iter.next();
            log.info("Starting job {} phase {}.", job.getName(), job.getPhase());
            job.startTask(this.workDir, this.workspace);
            if (! job.needsTask()) remaining--;
        }
    }

    /**
     * @return the set of incomplete jobs
     */
    private Set<RnaJob> getIncomplete() {
        Set<RnaJob> retVal = this.activeJobs.values().stream().filter(x -> x.getPhase() != RnaJob.Phase.DONE).collect(Collectors.toSet());
        return retVal;
    }

    /**
     * Check for jobs that are already running.
     */
    private void checkRunningJobs() {
        log.info("Checking for status of running jobs.");
        Map<String, String> runningJobMap = this.statusTask.getTasks();
        log.info("{} jobs are already running.", runningJobMap.size());
        for (Map.Entry<String, String> runningJob : runningJobMap.entrySet()) {
            // Get the job name and task ID.
            String jobName = runningJob.getKey();
            RnaJob job = this.activeJobs.get(jobName);
            if (job != null) {
                // Here the job is one we are monitoring.
                job.setTaskId(runningJob.getValue());
            }
        }
    }

    /**
     * Determine which jobs are already partially complete.
     */
    private void scanOutputDirectory() {
        log.info("Scanning output directory {}.", this.outDir);
        List<DirEntry> outputFiles = this.dirTask.list(this.outDir);
        this.updateCounter = 0;
        boolean fpkmFound = false;
        for (DirEntry outputFile : outputFiles) {
            if (outputFile.getType() == DirEntry.Type.JOB_RESULT) {
                // Here we have a possible job result.  Check the phase and adjust the job accordingly.
                String jobFolder = outputFile.getName();
                if (! this.checkJobResult(jobFolder, RnaJob.Phase.ALIGN))
                    this.checkJobResult(jobFolder, RnaJob.Phase.TRIM);
            } else if (outputFile.getName().contentEquals(RnaJob.FPKM_DIR)) {
                this.checkFpkmDirectory();
                fpkmFound = true;
            }
        }
        if (! fpkmFound) {
            log.info("Creating FPKM output directory.");
            MkDirTask mkdir = new MkDirTask(this.workDir, this.workspace);
            mkdir.make(RnaJob.FPKM_DIR, this.outDir);
        }
        log.info("Output directory scan complete. {} job updates recorded.", this.updateCounter);
    }

    /**
     * Process the FPKM directory to determine which FPKM files have already been copied.
     */
    private void checkFpkmDirectory() {
        RnaJob.Phase nextPhase = RnaJob.Phase.values()[RnaJob.Phase.COPY.ordinal() + 1];
        log.info("Scanning {} folder in {}.", RnaJob.FPKM_DIR, this.outDir);
        List<DirEntry> outputFiles = this.dirTask.list(this.outDir + "/" + RnaJob.FPKM_DIR);
        for (DirEntry outputFile : outputFiles) {
            if (outputFile.getType() == DirEntry.Type.TEXT) {
                String fileName = outputFile.getName();
                String jobName = RnaJob.Phase.COPY.checkSuffix(fileName);
                if (jobName != null) {
                    // Here we have a potential FPKM file.  Insure it's for one of our jobs.
                    RnaJob job = this.activeJobs.get(jobName);
                    if (job != null) {
                        job.mergeState(nextPhase);
                        log.info("Job {} updated by found FPKM file {}.", jobName, fileName);
                        this.updateCounter++;
                    }
                }
            }
        }
    }

    /**
     * Check a job result to see if it requires updating a job's state.
     *
     * @param jobFolder		result folder from the output directory
     * @param possible		phase to check for
     *
     * @return TRUE if the folder represents a job result for the indicated phase, else FALSE
     */
    private boolean checkJobResult(String jobFolder, RnaJob.Phase possible) {
        boolean retVal = false;
        RnaJob.Phase nextPhase = RnaJob.Phase.values()[possible.ordinal() + 1];
        // Check to see if this is a possible job result for the indicated phase.
        String jobName = possible.checkSuffix(jobFolder);
        if (jobName != null) {
            // This is a result of the appropriate type.
            retVal = true;
            // Check to see if it is one of ours.
            RnaJob job = this.activeJobs.get(jobName);
            if (job != null) {
                // It is.  Check for failure.  Otherwise, update the state.
                List<DirEntry> folderFiles = this.dirTask.list(this.outDir + "/." + jobFolder);
                if (folderFiles.stream().anyMatch(x -> x.getName().contentEquals("JobFailed.txt"))) {
                    log.warn("Job {} folder {} contains failure data.", jobName, jobFolder);
                    this.retryCounts.count(jobName);
                } else if (job.mergeState(nextPhase)) {
                    this.updateCounter++;
                    log.info("Job {} updated by completed task {}.", jobName, jobFolder);
                }
            }
        }
        return retVal;
    }

    @Override
    public String getPattern() {
        return this.readRegex;
    }

    @Override
    public String getLeftId() {
        return this.leftId;
    }

    @Override
    public File getWorkDir() {
        return this.workDir;
    }

    @Override
    public String getWorkspace() {
        return this.workspace;
    }

}
