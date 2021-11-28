/**
 *
 */
package org.theseed.rna.jobs;

import java.io.File;

import org.theseed.cli.CliService;
import com.github.cliftonlabs.json_simple.JsonArray;
import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This service class initiates trimming the FASTQ files to produce trimmed results.
 *
 * @author Bruce Parrello
 */
public class TrimService extends CliService {

    // FIELDS
    /** parameters for the trimming */
    private JsonObject parms;
    /** recipe constant */
    private static final JsonArray recipe = new JsonArray().addChain("Trim").addChain("FastQC");

    /**
     * Construct the trimming service request.
     *
     * @param job			job for which this service performs the trimming phase
     * @param workDir		wokring directory for temporary files
     * @param workspace		controlling workspace
     */
    public TrimService(RnaJob job, File workDir, String workspace) {
        super(job.getName(), workDir, workspace);
        // Now we need to build the parameters.
        this.parms = new JsonObject();
        this.parms.put("output_file", RnaJob.Phase.TRIM.getOutputName(job.getName()));
        this.parms.put("output_path", job.getOutDir());
        this.parms.put("recipe", recipe);
        job.getSource().store(this.parms);
    }

    @Override
    protected void startService() {
        this.submit("FastqUtils", this.parms);
    }

}
