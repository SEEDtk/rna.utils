/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbLoader;
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
 * --replace	if specified, existing samples will be deleted before processing
 *
 * @author Bruce Parrello
 *
 */
public class DbLoadProcessor extends BaseDbLoadProcessor implements MeasureComputer.IParms {

    /** RNA database from the file */
    private RnaData data;
    /** queue of measurement inserts */
    private Set<MeasurementDesc> measurements;
    /** list of measurers */
    private List<MeasureComputer> measurerList;


    // COMMAND-LINE OPTIONS

    /** types of measurement to perform */
    @Option(name = "--measure", aliases = { "-m" }, usage = "types of measurements to perform")
    private List<MeasureComputer.Type> measurers;

    /** RNA database file name */
    @Argument(index = 1, metaVar = "rnaData.ser", usage = "RNA database file to load", required = true)
    private File rnaFile;

    @Override
    protected void setDbLoadDefaults() {
        this.measurements = new HashSet<MeasurementDesc>(1000);
        this.measurers = new ArrayList<MeasureComputer.Type>();
    }

    @Override
    protected void validateDbLoadParms() throws IOException, ParseFailureException {
        if (! this.rnaFile.canRead())
            throw new FileNotFoundException("RNA data file " + this.rnaFile + " is not found or unreadable.");
        log.info("Reading RNA data from {}.", this.rnaFile);
        try {
            this.data = RnaData.load(this.rnaFile);
        } catch (ClassNotFoundException e) {
            throw new IOException("Probable version error in database: " + e.toString());
        }
        // Get the list of measurers.
        this.measurerList = this.measurers.stream().map(x -> x.create(this)).collect(Collectors.toList());
        log.info("{} measurement computers configured.", this.measurerList.size());
    }

    @Override
    protected void loadSamples(DbConnection db) throws Exception {
        // Create a loader for the samples.
        try (DbLoader jobLoader = DbLoader.batch(db, "RnaSample")) {
            this.initJobLoader(jobLoader);
            // Loop through the RNA samples in the file.
            final int n = this.data.size();
            for (int i = 0; i < n; i++) {
                this.loadSample(jobLoader, i);
            }
        }
        // Load the measurements we queued up from the samples.
        try (DbLoader measurementLoader = DbLoader.batch(db, "Measurement")) {
            // The genome ID is constant.
            measurementLoader.set("genome_id", this.getGenomeId());
            // Loop through the queued measurements, inserting.
            for (MeasurementDesc measureDesc : this.measurements) {
                measureDesc.storeData(measurementLoader);
                measurementLoader.insert();
            }
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
        jobLoader.set("base_count", (double) sample.getBaseCount());
        jobLoader.set("read_count", sample.getReadCount());
        jobLoader.set("quality", sample.getQuality());
        jobLoader.set("suspicious",  sample.isSuspicious());
        jobLoader.set("process_date", sample.getProcessingDate());
        // Now we need to fill in the project and pubmed.
        this.computeProjectInfo(jobLoader, sampleId);
        // Now copy the features and count the number of features with values.
        double[] featData = new double[this.getFeatureCount()];
        int featCount = 0;
        for (int i = 0; i < featData.length; i++) {
            String fid = this.getFeatureId(i);
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

    @Override
    protected Collection<String> getNewSamples() {
        return this.data.getSamples().stream().map(x -> x.getName()).collect(Collectors.toSet());
    }

}
