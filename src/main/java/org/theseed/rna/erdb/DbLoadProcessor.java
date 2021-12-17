/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.erdb.utils.BaseDbProcessor;
import org.theseed.io.TabbedLineReader;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbLoader;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.Relop;
import org.theseed.rna.RnaData;
import org.theseed.utils.ParseFailureException;

/**
 * This command loads an RNA database file into the live RNA SQL database.  The database must be initialized
 * and the target genome must already have been filled in by MetaLoadProcessor.
 *
 * The positional parameters are the target genome ID and the name of the RNA database file.
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
 * --ncbi		the name of a tab-delimited with headers containing (0) the sample ID, (1) the NCBI project ID,
 * 				and (2) the PUBMED ID of the associated paper
 *
 * @author Bruce Parrello
 *
 */
public class DbLoadProcessor extends BaseDbProcessor {

    /**
     * This class describes a single measurement.  Since measurements cannot be inserted until the
     * measured samples exist, we queue them and run them at the end.  This allows us to batch
     * the sample queries to improve performance.
     */
    private static class MeasurementDesc {

        /** ID of the sample being measured */
        protected String sampleId;
        /** type of measurement */
        protected String type;
        /** value of the measurement */
        protected double value;

        /**
         * Create a measurement.
         *
         * @param sample		ID of the sample being measured
         * @param typeName		type of measurement
         * @param measurement	value of measurement
         */
        public MeasurementDesc(String sample, String typeName, double measurement) {
            this.sampleId = sample;
            this.type = typeName;
            this.value = measurement;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((this.sampleId == null) ? 0 : this.sampleId.hashCode());
            result = prime * result + ((this.type == null) ? 0 : this.type.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof MeasurementDesc)) {
                return false;
            }
            MeasurementDesc other = (MeasurementDesc) obj;
            if (this.sampleId == null) {
                if (other.sampleId != null) {
                    return false;
                }
            } else if (!this.sampleId.equals(other.sampleId)) {
                return false;
            }
            if (this.type == null) {
                if (other.type != null) {
                    return false;
                }
            } else if (!this.type.equals(other.type)) {
                return false;
            }
            return true;
        }

        /**
         * Store this measurement in the specified insert statement
         *
         * @param loader	insert statement to update
         *
         * @throws SQLException
         */
        public void storeData(DbLoader loader) throws SQLException {
            loader.set("sample_id", sampleId);
            loader.set("value", this.value);
            loader.set("measure_type", this.type);
        }
    }
    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(DbLoadProcessor.class);
    /** RNA database from the file */
    private RnaData data;
    /** queue of measurement inserts */
    private Set<MeasurementDesc> measurements;
    /** list of feature IDs in the order expected by the database */
    private List<String> featureIndex;
    /** map of sample IDs to project IDs */
    private Map<String, String> projectMap;
    /** map of sample IDs to pubmed IDs */
    private Map<String, Integer> pubmedMap;
    /** match pattern for genome IDs */
    private static final Pattern GENOME_ID_PATTERN = Pattern.compile("\\d+\\.\\d+");

    // COMMAND-LINE OPTIONS

    /** NCBI attribute file name */
    @Option(name = "--ncbi", metaVar = "srrReport.tbl", usage = "name of a file containing pubmed and project data from NCBI")
    private File ncbiFile;

    /** target genome ID */
    @Argument(index = 0, metaVar = "genomeId", usage = "ID of genome to which the RNA was mapped", required = true)
    private String genomeId;

    /** RNA database file name */
    @Argument(index = 1, metaVar = "rnaData.ser", usage = "RNA database file to load", required = true)
    private File rnaFile;

    @Override
    protected void setDbDefaults() {
        this.measurements = new HashSet<MeasurementDesc>(1000);
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        if (! this.rnaFile.canRead())
            throw new FileNotFoundException("RNA data file " + this.rnaFile + " is not found or unreadable.");
        log.info("Reading RNA data from {}.", this.rnaFile);
        try {
            this.data = RnaData.load(this.rnaFile);
        } catch (ClassNotFoundException e) {
            throw new IOException("Probable version error in database: " + e.toString());
        }
        if (! GENOME_ID_PATTERN.matcher(this.genomeId).matches())
            throw new ParseFailureException("Invalid genome ID \"" + this.genomeId + "\".");
        // Process the NCBI maps.
        this.projectMap = new HashMap<String, String>(1000);
        this.pubmedMap = new HashMap<String, Integer>(1000);
        if (this.ncbiFile != null) {
            try (TabbedLineReader ncbiStream = new TabbedLineReader(this.ncbiFile)) {
                int keyCol = ncbiStream.findColumn("sample_id");
                int projCol = ncbiStream.findColumn("project");
                int pubCol = ncbiStream.findColumn("pubmed");
                for (TabbedLineReader.Line line : ncbiStream) {
                    String sampleId = line.get(keyCol);
                    String project = line.get(projCol);
                    if (! StringUtils.isBlank(project))
                        this.projectMap.put(sampleId, project);
                    String pubmed = line.get(pubCol);
                    if (! StringUtils.isBlank(pubmed))
                        this.pubmedMap.put(sampleId, Integer.valueOf(pubmed));
                }
            }
            log.info("{} samples with projects, {} with papers.", this.projectMap.size(), this.pubmedMap.size());
        }
        return true;
    }

    @Override
    protected void runDbCommand(DbConnection db) throws Exception {
        // Create the array of feature indices.
        this.featureIndex = new ArrayList<String>(4000);
        try (DbQuery query = new DbQuery(db, "Genome Feature")) {
            query.select("Feature", "fig_id", "seq_no");
            query.rel("Genome.genome_id", Relop.EQ);
            query.setParm(1, this.genomeId);
            Iterator<DbRecord> iter = query.iterator();
            while (iter.hasNext()) {
                DbRecord feat = iter.next();
                String fid = feat.getString("Feature.fig_id");
                int idx = feat.getInt("Feature.seq_no");
                this.featureIndex.add(idx, fid);
            }
        }
        if (this.featureIndex.isEmpty())
            throw new ParseFailureException("Genome ID \"" + this.genomeId + "\" is not found or has no features.");
        // Perform all the updates in a single transaction.
        try (var xact = db.new Transaction()) {
            // Create a loader for the samples.
            try (DbLoader jobLoader = DbLoader.batch(db, "RnaSample")) {
                // We don't have cluster data, so null the cluster ID.
                jobLoader.setNull("cluster_id");
                // Point all samples at the target genome.
                jobLoader.set("genome_id", this.genomeId);
                // Loop through the RNA samples in the file.
                final int n = this.data.size();
                for (int i = 0; i < n; i++) {
                    this.loadSample(jobLoader, i);
                }
            }
            // Load the measurements we queued up from the samples.
            try (DbLoader measurementLoader = DbLoader.batch(db, "Measurement")) {
                // The genome ID is constant.
                measurementLoader.set("genome_id", this.genomeId);
                // Loop through the queued measurements, inserting.
                for (MeasurementDesc measureDesc : this.measurements) {
                    measureDesc.storeData(measurementLoader);
                    measurementLoader.insert();
                }
            }
            // Commit the updates.
            xact.commit();
        }
    }

    /**
     * Load a sample into the RnaSample table and queue up any measurements we have.
     *
     * @param jobLoader		loader for inserting sample records
     * @param jobIdx		column index of the sample in the RNA database
     *
     * @throws SQLException
     */
    private void loadSample(DbLoader jobLoader, int jobIdx) throws SQLException {
        // Get the sample descriptor and the sample ID.
        RnaData.JobData sample = this.data.getJob(jobIdx);
        String sampleId = sample.getName();
        // Fill in the fields.
        jobLoader.set("sample_id", sampleId);
        jobLoader.set("base_count", (int) sample.getBaseCount());
        jobLoader.set("read_count", sample.getReadCount());
        jobLoader.set("quality", sample.getQuality());
        jobLoader.set("suspicious",  sample.isSuspicious());
        jobLoader.set("project_id", this.projectMap.get(sampleId));
        Integer pubmed = this.pubmedMap.get(sampleId);
        if (pubmed == null)
            jobLoader.setNull("pubmed");
        else
            jobLoader.set("pubmed", (int) pubmed);
        jobLoader.set("process_date", sample.getProcessingDate());
        // Now copy the features and count the number of features with values.
        double[] featData = new double[this.featureIndex.size()];
        int featCount = 0;
        for (int i = 0; i < featData.length; i++) {
            String fid = this.featureIndex.get(i);
            RnaData.Row featRow = this.data.getRow(fid);
            if (featRow == null || ! featRow.isGood(jobIdx))
                featData[i] = Double.NaN;
            else {
                featData[i] = featRow.getWeight(jobIdx).getWeight();
                featCount++;
            }
        }
        // Store the feature data and count.
        jobLoader.set("feat_data", featData);
        jobLoader.set("feat_count", featCount);
        // Insert the record.
        jobLoader.insert();
        // Now we queue the measurements.
        this.storeMeasurement(sampleId, "OD/600", sample.getOpticalDensity());
        this.storeMeasurement(sampleId, "thr_production", sample.getProduction());
    }

    /**
     * Queue a measurement for the specified sample.
     *
     * @param sampleId		sample ID
     * @param type			measurement type
     * @param value			measurement value
     */
    private void storeMeasurement(String sampleId, String type, double value) {
        if (Double.isFinite(value)) {
            MeasurementDesc desc = new MeasurementDesc(sampleId, type, value);
            this.measurements.add(desc);
        }
    }

}
