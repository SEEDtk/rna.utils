/**
 *
 */
package org.theseed.rna.jobs;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An RNA Seq group is an organized group of expression data files.  All the expression data must be
 * for the same genome and destined for the same output directory.  We require that all the data
 * in the output directory be for the same group. The various RNA-processing commands use the group
 * objects to find and identify all the input files or samples and set up the various jobs to convert them
 * to a final database.
 *
 * @author Bruce Parrello
 *
 */
public abstract class RnaSeqGroup {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(RnaSeqGroup.class);
    /** PATRIC output directory */
    private String outDir;
    /** PATRIC ID of base genome for mapping */
    private String genomeId;

    /**
     * This enumeration represents the different group types.
     */
    public static enum Type {
        PATRIC_DIR {

            @Override
            public RnaSeqGroup create(IParms processor) {
                return new PatricRnaSeqGroup(processor);
            }

        }, SRA_FILE {

            @Override
            public RnaSeqGroup create(IParms processor) {
                return new SraRnaSeqGroup(processor);
            }

        };

        public abstract RnaSeqGroup create(IParms processor);

    }

    /**
     * This interface specifies the parameter callbacks required of a processor that creates
     * RnaSeqGroup instances.
     */
    public interface IParms {

        /**
         * @return the filename pattern for FASTQ files
         */
        public String getPattern();

        /**
         * @return the left-file identifier for FASTQ files
         */
        public String getLeftId();

        /**
         * @return the name of the work directory
         */
        public File getWorkDir();

        /**
         * @return the name of the user's workspace
         */
        public String getWorkspace();

    }

    /**
     * This method scans the source and computes the set of jobs related to the group.
     *
     * @param inFile	source file or directory for the jobs
     *
     * @return a map of the set of jobs for this group, keyed by job name
     *
     * @throws IOException
     */
    public abstract Map<String, RnaJob> getJobs(String inFile) throws IOException;

    /**
     * @return the PATRIC output directory
     */
    public String getOutDir() {
        return this.outDir;
    }
    /**
     * Specify a new PATRIC output directory
     *
     * @param outDir the outDir to set
     */
    public void setOutDir(String outDir) {
        this.outDir = outDir;
    }

    /**
     * @return the ID of the base genome
     */
    public String getGenomeId() {
        return this.genomeId;
    }

    /**
     * Specify a new base genome for mapping.
     *
     * @param genomeId 	the ID of the new base genome
     */
    public void setGenomeId(String genomeId) {
        this.genomeId = genomeId;
    }

}
