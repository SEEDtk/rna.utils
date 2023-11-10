/**
 *
 */
package org.theseed.rna.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.rna.RnaData;

/**
 * This method splits a single RNA Seq expression database into a separate database for each cluster.
 * Each database will be named "XXXX.tpm.ser", where "XXXX" is the cluster name.
 *
 * The positional parameters are the input RNA Seq expression database and the output directory name.
 * The standard input should contain the TABULAR-format cluster report for the samples in the incoming
 * database.
 *
 * A directory file will also be written to the output directory.  This directory file "clusters.dir.tbl"
 * will contain the name and size of each cluster.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i 	name of the input file containing cluster data (if not STDIN)
 *
 * --clear		if specified, the output directory will be erased before processing
 * --baselines	if specified, the baseline for each cluster will be computed from the cluster itself
 *
 * @author Bruce Parrello
 *
 */
public class ClusterSplitProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(ClusterSplitProcessor.class);
    /** source RNA seq database */
    private RnaData data;
    /** map of cluster IDs to sample IDs */
    private Map<String, Set<String>> clusterMap;

    // COMMAND-LINE OPTIONS

    /** input file (if not STDIN) */
    @Option(name = "--input", aliases = { "-i" }, metaVar = "clusters.tbl", usage = "the name of the cluster input file (if not STDIN)")
    private File inFile;

    /** if specified, the output directory will be emptied before processing */
    @Option(name = "--clear", usage = "clear the output directory before starting")
    private boolean clearFlag;

    /** if specified, the baseline for each output cluster will be recomputed from the cluster itself */
    @Option(name = "--baselines", usage = "recompute the baseline for each cluster")
    private boolean reBaseline;

    /** source RNA Seq expression database */
    @Argument(index = 0, metaVar = "rnaData.ser", usage = "input RNA Seq expression database", required = true)
    private File rnaFile;

    /** output directory */
    @Argument(index = 1, metaVar = "outDir", usage = "output directory for cluster databases", required = true)
    private File outDir;

    @Override
    protected void setDefaults() {
        this.inFile = null;
        this.clearFlag = false;
        this.reBaseline = false;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Verify that the RNA database exists.
        if (! this.rnaFile.canRead())
            throw new FileNotFoundException("Rna data file " + this.rnaFile + " is not found or invalid.");
        // Process the input file.
        Map<String, Set<String>> clMap = RnaData.readClusterMap(this.inFile);
        this.clusterMap = clMap;
        // Read the RNA database.
        log.info("Loading RNA database from {}.", this.rnaFile);
        try {
            this.data = RnaData.load(this.rnaFile);
        } catch (ClassNotFoundException e) {
            throw new IOException("Class error reading RNA data: " + e.getMessage());
        }
        log.info("{} samples found for {} features.", this.data.size(), this.data.rows());
        // Set up the output directory.
        if (! this.outDir.isDirectory()) {
            log.info("Creating output directory {}.", this.outDir);
            FileUtils.forceMkdir(this.outDir);
        } else if (this.clearFlag) {
            log.info("Erasing output directory {}.", this.outDir);
            FileUtils.cleanDirectory(this.outDir);
        } else
            log.info("Output files will be written to {}.", this.outDir);
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Open the directory file.
        File outFile = new File(this.outDir, "clusters.dir.tbl");
        try (PrintWriter writer = new PrintWriter(outFile)) {
            // Write the directory header.
            writer.println("cluster_id\tcount");
            // Loop through the clusters.
            for (Map.Entry<String, Set<String>> clEntry : this.clusterMap.entrySet()) {
                String clId = clEntry.getKey();
                File clusterFile = new File(this.outDir, clId + ".tpm.ser");
                Set<String> members = clEntry.getValue();
                log.info("Processing cluster {} (size {}) for file {}.", clId, members.size(), clusterFile);
                RnaData clusterRna = this.data.getSubset(members);
                if (this.reBaseline) {
                    log.info("Recomputing baselines for the cluster.");
                    for (RnaData.Row row : clusterRna) {
                        DescriptiveStatistics stats = RnaData.getStats(row);
                        row.getFeat().setBaseLine(stats.getMean());
                    }
                }
                clusterRna.save(clusterFile);
                writer.format("%s\t%d%n", clId, members.size());
            }
        }
    }

}
