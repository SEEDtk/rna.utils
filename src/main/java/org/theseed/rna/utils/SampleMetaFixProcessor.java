/**
 *
 */
package org.theseed.rna.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.LineReader;
import org.theseed.samples.SampleFileUtils;
import org.theseed.samples.SampleMeta;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This script reads in data files containing production and optical density data and uses it to update the
 * sampleMeta.tbl file.  It is used in cases where the data is not available at the time the original
 * RNA seq database is created.
 *
 * The positional parameter is the name of a file containing the sample IDs to be updated.
 *
 * A production data file contains sample IDs in the first column and production values in the other columns.
 * Each data column header should be a time point value.  This is a tab-delimited file with headers
 *
 * An optical density data file contains two parallel matrices.  The first contains the sample IDs and the second
 * contains the corresponding optical densities.  A double slash separates the two matrices.  In this case
 * the time point must be specified externally.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --time		default time point for optical density data (default "9")
 * --samples	name of the sampleMeta.tbl file (default to "sampleMeta.tbl" in the current directory)
 * --prod		name of the production data file; if omitted, the production data is not updated
 * --od			name of the optical density file; if omitted, the optical density data is not updated
 *
 * @author Bruce Parrello
 *
 */
public class SampleMetaFixProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SampleMetaFixProcessor.class);
    /** SampleMeta file manager */
    private SampleFileUtils samples;
    /** set of likely sample IDs */
    private Set<String> likelySamples;
    /** time point string validator */
    private static final Pattern TIME_PATTERN = Pattern.compile("\\d+(?:p5)?");

    // COMMAND-LINE OPTIONS

    /** default time point */
    @Option(name = "--time", metaVar = "12", usage = "default time point for optical density data")
    private String timeDefault;

    /** sample metadata file */
    @Option(name = "--samples", metaVar = "sampleMeta.tbl", usage = "name of the sample metadata file")
    private File sampleFile;

    /** production file name */
    @Option(name = "--prod", metaVar = "prodFile.tbl", usage = "name of the production data file")
    private File prodFile;

    /** optical density file name */
    @Option(name = "--od", metaVar = "opticalFile.tbl", usage = "name of the optical density matrix file")
    private File optFile;

    /** file containing list of sample IDs */
    @Argument(index = 0, metaVar = "sampleId.txt", usage = "file containing list of likely sample IDs", required = true)
    private File sampleListFile;

    @Override
    protected void setDefaults() {
        this.timeDefault = "9";
        this.sampleFile = new File(System.getProperty("user.dir"), "sampleMeta.tbl");
        this.prodFile = null;
        this.optFile = null;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Insure all the input files exist.
        if (! this.sampleFile.canRead())
            throw new FileNotFoundException("Sample metadata file " + this.sampleFile + " is not found or unreadable.");
        if (this.prodFile != null && ! this.prodFile.canRead())
            throw new FileNotFoundException("Production data file " + this.prodFile + " is not found or unreadable.");
        if (this.optFile != null && ! this.optFile.canRead())
            throw new FileNotFoundException("Optical density file " + this.optFile + " is not found or unreadable.");
        if (! this.sampleListFile.canRead())
            throw new FileNotFoundException("Sample list file " + this.sampleListFile + " is not found or unreadable.");
        // Validate the time point.
        if (! TIME_PATTERN.matcher(this.timeDefault).matches())
            throw new ParseFailureException("Default time point " + this.timeDefault + " is not valid.");
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Get the list of likely sample IDs.
        this.likelySamples = LineReader.readSet(this.sampleListFile);
        log.info("{} likely sample IDs found.", this.likelySamples.size());
        // Read the existing sample metadata.
        this.samples = new SampleFileUtils(this.sampleFile);
        log.info("{} samples found in {}.", this.samples.size(), this.sampleFile);
        // Create the likely-sample map.
        Map<String, SampleMeta> sampleMap = this.samples.getLikelyMap(this.likelySamples);
        log.info("{} of {} likely samples map to metadata file.", sampleMap.size(), this.likelySamples.size());
        // Check for a production data file.
        if (this.prodFile != null) {
            log.info("Processing production data from {}.", this.prodFile);
            this.samples.processProductionFile(this.prodFile, sampleMap);
        }
        // Check for an optical density file.
        if (this.optFile != null) {
            log.info("Processing optical density data from {}.", this.optFile);
            this.samples.processDensityMatrixFile(this.optFile, this.timeDefault, sampleMap);
        }
        // Write the updated samples.
        this.samples.save(this.sampleFile);
        log.info("Updated samples saved to {}.", this.sampleFile);
    }
}
