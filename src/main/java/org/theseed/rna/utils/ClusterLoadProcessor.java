/**
 *
 */
package org.theseed.rna.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.clusters.Cluster;
import org.theseed.clusters.ClusterGroup;
import org.theseed.clusters.methods.ClusterMergeMethod;
import org.theseed.erdb.utils.BaseDbProcessor;
import org.theseed.erdb.utils.DbCollectors;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbLoader;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.Relop;
import org.theseed.java.erdb.SqlBuffer;
import org.theseed.utils.ParseFailureException;

/**
 * This is a very complicated database loader that updates the sample clusters for a genome in the
 * RNA database.  It takes as input the sample correlations produced by the "sampleCorr" subcommand.
 * These are used to form new clusters using the cutoff specified by the user.  The existing clusters
 * are then deleted.  This nulls out the pointers in the RnaSample table.  We then add new cluster
 * records and update the cluster IDs in the RnaSample records.
 *
 * The positional parameters are the ID of the target genome and the sample correlation input file.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 * --min		minimum acceptable clustering level (default 0.94)
 * --method		clustering method (default COMPLETE)
 *
 * @author Bruce Parrello
 *
 */
public class ClusterLoadProcessor extends BaseDbProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(ClusterLoadProcessor.class);
    /** cluster group manager */
    private ClusterGroup clusters;
    /** estimated number of datapoints */
    private int dataPoints;

    // COMMAND-LINE OPTIONS

    /** minimum similarity eligible for clustering */
    @Option(name = "--min", metaVar = "0.90", usage = "minimum similarity allowed for clustering")
    private double minSim;

    /** clustering method */
    @Option(name = "--method", usage = "clustering method")
    private ClusterMergeMethod method;

    /** genome ID to cluster */
    @Argument(index = 0, metaVar = "genomeId", usage = "ID of the genome whose samples should be re-clustered",
            required = true)
    private String genomeId;

    /** correlation input file name */
    @Argument(index = 1, metaVar = "sampleCorr.tbl", usage = "sample correlation input file",
            required = true)
    private File corrFile;

    @Override
    protected void setDbDefaults() {
        this.minSim = 0.94;
        this.method = ClusterMergeMethod.COMPLETE;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // First, insure that the minimum clustering level is valid.
        if (this.minSim > 1.0 || this.minSim <= 0.0)
            throw new ParseFailureException("Minimum similarity must be between 0 and 1.");
        // Now verify we can read the correlation file.
        if (! this.corrFile.canRead())
            throw new FileNotFoundException("Correlation file " + this.corrFile + " is not found or unreadable.");
        // Estimate the number of samples being correlated.
        this.dataPoints = (int) Math.sqrt(this.corrFile.length() / 25 / 2) + 1;
        return true;
    }

    @Override
    protected void runDbCommand(DbConnection db) throws Exception {
        // Check the genome ID.
        DbRecord genomeRecord = db.getRecord("Genome", this.genomeId);
        if (genomeRecord == null)
            throw new ParseFailureException("Invalid genome ID " + this.genomeId + ": not found in database.");
        log.info("Processing clusters for genome {}.", this.genomeId);
        // Now we create the cluster groups and merge them.
        log.info("Reading correlations from file {}.", this.corrFile);
        this.clusters = new ClusterGroup(this.dataPoints, this.method);
        this.clusters.load(this.corrFile, "1", "2", "3", false);
        if (this.clusters.size() < 2)
            throw new IOException("Two few samples to cluster.");
        // Now we validate the samples to insure they belong to our genome.
        this.validateSamples(db);
        log.info("Merging samples into clusters.");
        int mergeCount = 0;
        while (this.clusters.merge(this.minSim)) {
            mergeCount++;
            if (log.isInfoEnabled() && mergeCount % 100 == 0)
                log.info("{} merges performed.", mergeCount);
        }
        log.info("{} merges completed resulting in {} clusters.", mergeCount, this.clusters.size());
        // Now we have the clusters defined.  We need to update the database.  This is done inside a
        // transaction.
        try (var xact = db.new Transaction()) {
            // The first step is to delete and re-create the cluster records.
            this.deleteOldClusters(db);
            // The clusters are assigned numbers, in order.  The map below will map each cluster
            // ID to its member list.
            Map<String, Collection<String>> memberMap =
                    new HashMap<String, Collection<String>>(this.clusters.size() * 4 / 3);
            // Now we create the new clusters.  We create them all first, so we can batch the
            // inserts.  If we updated the sample records at the same time, we would have to
            // insert one at a time.
            log.info("Creating cluster records in the database.");
            try (DbLoader clusterLoader = DbLoader.batch(db, "SampleCluster")) {
                // This will be the cluster index.
                int clI = 0;
                for (Cluster cluster : this.clusters.getClusters()) {
                    String cluster_id = String.format("%s:CL%05d", this.genomeId, clI + 1);
                    clusterLoader.set("cluster_id", cluster_id);
                    Collection<String> members = cluster.getMembers();
                    clusterLoader.set("height", cluster.getHeight());
                    clusterLoader.set("numSamples", members.size());
                    clusterLoader.set("score", cluster.getScore());
                    // Save the member list.
                    memberMap.put(cluster_id, members);
                    // Insert the cluster record.
                    clusterLoader.insert();
                    // Set up for the next one.
                    clI++;
                    log.info("Cluster {} created.", cluster_id);
                }
            }
            // Now we store the cluster IDs in the sample records.
            SqlBuffer updateBuffer = new SqlBuffer(db).append("UPDATE RnaSample SET ").quote("cluster_id")
                    .append(" = ").appendMark().append(" WHERE sample_id = ").appendMark();
            try (PreparedStatement stmt = db.createStatement(updateBuffer)) {
                int updateCount = 0;
                // We use this counter to control batching.
                int batchSize = 0;
                // Loop through the clusters.
                for (Map.Entry<String, Collection<String>> clEntry : memberMap.entrySet()) {
                    // Update the cluster ID to this cluster's ID.
                    stmt.setString(2, clEntry.getKey());
                    // Perform the update for each member.
                    for (String sampleId : clEntry.getValue()) {
                        // Insure there is room in the batch.
                        if (batchSize >= 100) {
                            stmt.executeBatch();
                            batchSize = 0;
                            log.info("{} samples updated.", updateCount);
                        }
                        // Batch this update.
                        stmt.setString(1, sampleId);
                        stmt.addBatch();
                        batchSize++;
                        updateCount++;
                    }
                }
                // Execute the residual.
                if (batchSize > 0)
                    stmt.executeBatch();
            }
            // Commit the changes.
            xact.commit();
        }
    }

    /**
     * Verify that each sample in the cluster table belongs to the caller-specified genome.
     *
     * @param db	database connection
     *
     * @throws SQLException
     * @throws ParseFailureException
     */
    private void validateSamples(DbConnection db) throws SQLException, ParseFailureException {
        log.info("Validating the input samples.");
        // Create a set to contain all the samples currently belonging to the genome.
        Set<String> sampleSet;
        try (DbQuery gSamples = new DbQuery(db, "RnaSample")) {
            gSamples.select("RnaSample", "sample_id");
            gSamples.rel("RnaSample.genome_id", Relop.EQ);
            gSamples.setParm(1, this.genomeId);
            sampleSet = gSamples.stream().collect(DbCollectors.set("RnaSample.sample_id"));
        }
        // Now verify that each loaded sample from the correlation files is in the sample set.
        List<String> invalid = this.clusters.getClusters().stream().map(x -> x.getId()).
                filter(x -> ! sampleSet.contains(x)).sorted().collect(Collectors.toList());
        if (! invalid.isEmpty()) {
            // If there are a small number of bad ones, we list them; otherwise, we use a general message.
            if (invalid.size() < 10)
                throw new ParseFailureException("The following samples do not belong to genome " +
                        this.genomeId + ": " + StringUtils.join(invalid, ", "));
            else
                throw new ParseFailureException(Integer.toString(invalid.size()) + " samples do not belong to genome "
                        + this.genomeId + ".");
        }
    }

    /**
     * Delete the old sample clusters.
     *
     * @param db	database connection
     *
     * @throws SQLException
     */
    protected void deleteOldClusters(DbConnection db) throws SQLException {
        log.info("Deleting old clusters for {}.", this.genomeId);
        SqlBuffer deleteBuffer = new SqlBuffer(db).append("DELETE FROM ").quote("SampleCluster")
                .append(" WHERE ").quote("SampleCluster", "cluster_id").append(" LIKE ").appendMark();
        try (PreparedStatement deleteStmt = db.createStatement(deleteBuffer)) {
            deleteStmt.setString(1, this.genomeId + ":%");
            deleteStmt.execute();
        }
    }

}
