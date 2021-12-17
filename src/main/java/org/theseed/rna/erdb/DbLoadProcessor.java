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
import java.util.stream.Collectors;

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
import org.theseed.utils.PatternMap;

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
 * -m	type of special measurements to perform (may occur multiple times
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 * --ncbi		the name of a tab-delimited file with headers containing (0) the sample ID, (1) the NCBI project ID,
 * 				and (2) the PUBMED ID of the associated paper
 * --proj		the name of a tab-delimited file with headers containing (0) a regex for the sample ID, (1) the project
 * 				ID to use, and (2) the PUBMED ID of the associated paper
 *
 * @author Bruce Parrello
 *
 */
public class DbLoadProcessor extends BaseDbProcessor implements MeasureComputer.IParms {

    /**
     * This object describes project/paper information for a sample.
     */
    public static class ProjInfo {

        /** project ID, or NULL if none */
        private String projectId;
        /** pubmed ID, or 0 if none */
        private int pubmedId;

        /**
         * Create a project info record from the project and pubmed strings.
         *
         * @param project	project string, or empty if no project
         * @param pubmed	pubmed ID number, or empty if no associated paper
         */
        public ProjInfo(String project, String pubmed) {
            this.projectId = (StringUtils.isBlank(project) ? null : project);
            this.pubmedId = (StringUtils.isBlank(pubmed) ? 0 : Integer.valueOf(pubmed));
        }

        /**
         * Store this project information in the specified loader.
         *
         * @param jobLoader		sample record loader
         *
         * @throws SQLException
         */
        public void store(DbLoader jobLoader) throws SQLException {
            jobLoader.set("project_id", this.projectId);
            if (this.pubmedId == 0)
                jobLoader.setNull("pubmed");
            else
                jobLoader.set("pubmed",  this.pubmedId);
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
    /** map of sample IDs to project info */
    private Map<String, ProjInfo> stringMap;
    /** map of sample ID patterns to project info */
    private PatternMap<ProjInfo> patternMap;
    /** list of measurers */
    private List<MeasureComputer> measurerList;
    /** match pattern for genome IDs */
    private static final Pattern GENOME_ID_PATTERN = Pattern.compile("\\d+\\.\\d+");

    // COMMAND-LINE OPTIONS

    /** NCBI attribute file name */
    @Option(name = "--ncbi", metaVar = "srrReport.tbl", usage = "name of a file containing pubmed and project data from NCBI")
    private File ncbiFile;

    /** project pattern file name */
    @Option(name = "--proj", metaVar = "patterns.tbl", usage = "name of a file containing pubmed and project data based on sample ID regex patterns")
    private File patternFile;

    /** types of measurement to perform */
    @Option(name = "--measure", aliases = { "-m" }, usage = "types of measurements to perform")
    private List<MeasureComputer.Type> measurers;

    /** if specified, existing samples will be deleted before adding the new ones */
    @Option(name = "--replace", usage = "if specified, existing copies of the named samples will be deleted before loading")
    private boolean replaceFlag;

    /** target genome ID */
    @Argument(index = 0, metaVar = "genomeId", usage = "ID of genome to which the RNA was mapped", required = true)
    private String genomeId;

    /** RNA database file name */
    @Argument(index = 1, metaVar = "rnaData.ser", usage = "RNA database file to load", required = true)
    private File rnaFile;

    @Override
    protected void setDbDefaults() {
        this.measurements = new HashSet<MeasurementDesc>(1000);
        this.measurers = new ArrayList<MeasureComputer.Type>();
        this.replaceFlag = false;
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
        // Process the project-data maps.
        this.stringMap = new HashMap<String, ProjInfo>(1000);
        if (this.ncbiFile != null) {
            try (TabbedLineReader ncbiStream = new TabbedLineReader(this.ncbiFile)) {
                int keyCol = ncbiStream.findColumn("sample_id");
                int projCol = ncbiStream.findColumn("project");
                int pubCol = ncbiStream.findColumn("pubmed");
                for (TabbedLineReader.Line line : ncbiStream) {
                    String sampleId = line.get(keyCol);
                    String project = line.get(projCol);
                    String pubmed = line.get(pubCol);
                    this.stringMap.put(sampleId, new ProjInfo(project, pubmed));
                }
            }
            log.info("{} sample IDs found in {}.", this.stringMap.size(), this.ncbiFile);
        }
        this.patternMap = new PatternMap<ProjInfo>();
        if (this.patternFile != null) {
            try (TabbedLineReader patternStream = new TabbedLineReader(this.patternFile)) {
                int keyCol = patternStream.findColumn("sample_id");
                int projCol = patternStream.findColumn("project");
                int pubCol = patternStream.findColumn("pubmed");
                for (TabbedLineReader.Line line : patternStream) {
                    String pattern = line.get(keyCol);
                    String project = line.get(projCol);
                    String pubmed = line.get(pubCol);
                    this.patternMap.add(pattern, new ProjInfo(project, pubmed));
                }
            }
            log.info("{} sample patterns found in {}.", this.patternMap.size(), this.patternFile);
        }
        // Get the list of measurers.
        this.measurerList = this.measurers.stream().map(x -> x.create(this)).collect(Collectors.toList());
        log.info("{} measurement computers configured.", this.measurerList.size());
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
        // If we are replacing, we delete the samples first.  We do this outside of the main transaction, to avoid
        // locking problems.
        if (this.replaceFlag)
            this.deleteOldSamples(db);
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
     * Delete any copies of the RNA samples that might still be in the database.
     *
     * @param db	database from which the samples are to be deleted
     *
     * @throws SQLException
     */
    private void deleteOldSamples(DbConnection db) throws SQLException {
        try (var xact = db.new Transaction()) {
            // Get the list of sample IDs.
            Set<String> samples = this.data.getSamples().stream().map(x -> x.getName()).collect(Collectors.toSet());
            log.info("Deleting existing copies of {} samples.", samples.size());
            db.deleteRecords("RnaSample", samples);
            // Lock the deletes.
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
        jobLoader.set("process_date", sample.getProcessingDate());
        // Now we need to fill in the project and pubmed.  First, check
        // for an entry in the string map, then the pattern map.
        ProjInfo proj = this.stringMap.get(sampleId);
        if (proj == null)
            proj = this.patternMap.get(sampleId);
        if (proj != null)
            proj.store(jobLoader);
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
        for (MeasureComputer m : this.measurerList)
            this.measurements.addAll(m.measureSample(sampleId, data));
    }

}
