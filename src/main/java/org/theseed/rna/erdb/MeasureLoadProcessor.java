/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.erdb.utils.BaseDbProcessor;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbLoader;
import org.theseed.utils.ParseFailureException;

/**
 * This command loads measurements into the RNA Seq database.  It takes as input a measurement
 * file containing the basic measurements to load.  Additional measurement types may be specified,
 * but they will only be applied to samples listed in the measurement file.
 *
 * The first positional parameter is the ID of the genome for the samples and the second is the name
 * of the measurement file.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -m	additional measurement type (may be specified multiple times
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 *
 *
 * @author Bruce Parrello
 *
 */
public class MeasureLoadProcessor extends BaseDbProcessor implements MeasureComputer.IParms {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(MeasureLoadProcessor.class);
    /** list of additional measurement processors */
    private List<MeasureComputer> computers;
    /** list of samples to process */
    private Collection<String> samples;

    // COMMAND-LINE OPTIONS

    /** additional measurement types (multiple) */
    @Option(name = "--mType", aliases = { "-m" }, usage = "additional measurement types")
    private List<MeasureComputer.Type> mTypes;

    /** parent genome ID */
    @Argument(index = 0, metaVar = "genomeId", usage = "ID of the sample genomes", required = true)
    private String genomeId;

    /** measurement file */
    @Argument(index = 1, metaVar = "measureFile.tbl", usage = "main measurement file", required = true)
    private File measureFile;

    @Override
    protected void setDbDefaults() {
        this.mTypes = new ArrayList<MeasureComputer.Type>();
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        if (! this.measureFile.canRead())
            throw new FileNotFoundException("Measurement file " + this.measureFile + " is not found or unreadable.");
        // Get the list of additional types.  Note that if the user specifies FILE again we just ignore it.
        this.computers = new ArrayList<MeasureComputer>(this.mTypes.size() + 1);
        for (MeasureComputer.Type type : this.mTypes) {
            if (type != MeasureComputer.Type.FILE)
                this.computers.add(type.create(this));
        }
        // Add the file type.  This reads in all the sample IDs.
        log.info("Reading measurement file.");
        FileMeasureComputer fileComputer = new FileMeasureComputer(this);
        this.computers.add(fileComputer);
        this.samples = fileComputer.getSamples();
        return true;
    }

    @Override
    protected void runDbCommand(DbConnection db) throws Exception {
        try (DbLoader measureLoader = DbLoader.batch(db, "Measurement")) {
            log.info("Processing samples.  Target genome ID is {}.", this.genomeId);
            measureLoader.set("genome_id", this.genomeId);
            // Loop through the samples.
            int count = 0;
            int mCount = 0;
            for (String sample_id : this.samples) {
                // Only proceed if the sample exists.
                if (! db.checkForRecord("RnaSample", sample_id))
                    log.warn("Sample {} not found in database.", sample_id);
                else {
                    for (MeasureComputer computer : computers) {
                        Collection<MeasurementDesc> measures = computer.measureSample(sample_id);
                        for (MeasurementDesc measure : measures) {
                            measure.storeData(measureLoader);
                            measureLoader.insert();
                            mCount++;
                        }
                    }
                    count++;
                    if (log.isInfoEnabled() && count % 20 == 0)
                        log.info("{} samples processed.  {} measurements stored.", count, mCount);
                }
            }
            log.info("{} total samples with {} measurements.", count, mCount);
        }
    }

    @Override
    public File getMeasureFile() {
        return this.measureFile;
    }

}
