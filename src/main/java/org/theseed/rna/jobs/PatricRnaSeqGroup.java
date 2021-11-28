/**
 *
 */
package org.theseed.rna.jobs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.theseed.cli.DirEntry;
import org.theseed.cli.DirTask;
import org.theseed.cli.RnaSource;

/**
 * This RNA Seq input group represents a set of paired-end files in a PATRIC workspace directory.
 *
 * A regular expression is used to identify the FASTQ files, and a substring is used to distinguish
 * the left-end file from the right-end file.  The substring must match the second group from the
 * regular expression.  The first group will be the job name.
 *
 * The jobs produced all have Paired RNA sources.
 *
 * @author Bruce Parrello
 *
 */
public class PatricRnaSeqGroup extends RnaSeqGroup {

    // FIELDS
    /** file-name pattern */
    private Pattern readPattern;
    /** left-file identifier */
    private String leftId;
    /** get-directory task for workspace directories */
    private DirTask dirTask;

    public PatricRnaSeqGroup(RnaSeqGroup.IParms processor) {
        this.readPattern = Pattern.compile(processor.getPattern());
        this.leftId = processor.getLeftId();
        // Set up the PATRIC directory reader.
        this.dirTask = new DirTask(processor.getWorkDir(), processor.getWorkspace());
    }

    @Override
    public Map<String, RnaJob> getJobs(String inDir) {
        log.info("Scanning input directory {}.", inDir);
        List<DirEntry> inputFiles = this.dirTask.list(inDir);
        Map<String, RnaJob> retVal = new HashMap<String, RnaJob>((inputFiles.size() * 4 + 2) / 3);
        for (DirEntry inputFile : inputFiles) {
            if (inputFile.getType() == DirEntry.Type.READS) {
                // Here we have a file that could be used as input.
                Matcher m = this.readPattern.matcher(inputFile.getName());
                if (m.matches()) {
                    // Here the file is a good one.  Get the job for it.
                    String jobName = m.group(1);
                    boolean isLeft = m.group(2).contentEquals(this.leftId);
                    RnaJob job = retVal.computeIfAbsent(jobName,
                            x -> new RnaJob(x, this.getOutDir(), this.getGenomeId()));
                    // Store the file.
                    RnaSource.Paired source = getSource(job);
                    String fullName = inDir + "/" + inputFile.getName();
                    if (isLeft)
                        source.storeLeft(fullName);
                    else
                        source.storeRight(fullName);
                }
            }
        }
        return retVal;
    }

    /**
     * This method gets a paired-end RnaSource associated with the specified job.  If there is not currently
     * such a source, it will create one.
     *
     * @param job	relevant RnaJob
     *
     * @return a paired-end RnaSource associated with the specified job
     */
    private static RnaSource.Paired getSource(RnaJob job) {
        RnaSource source = job.getSource();
        RnaSource.Paired retVal;
        if (source == null) {
            // Here we have to create a source.
            retVal = new RnaSource.Paired();
            job.setSource(retVal);
        } else {
            // Here we have to convert the source.
            retVal = (RnaSource.Paired) source;
        }
        return retVal;
    }

}
