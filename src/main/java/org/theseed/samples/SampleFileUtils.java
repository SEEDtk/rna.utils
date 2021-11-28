/**
 *
 */
package org.theseed.samples;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RegExUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.LineReader;
import org.theseed.io.TabbedLineReader;

/**
 * This class contains utilities for reading production and optical density data to be plugged into SampleMeta
 * objects.
 *
 * The constructor creates a list of SampleMeta objects from a SampleMeta file.  A set of samples of interest is
 * passed in, and these are used to identify the specific samples to be used when an old-format strain and time point
 * is specified.
 *
 * Methods are provided to get optical density data from an optical density matrix file and production data from a
 * time-series production file.
 *
 * @author Bruce Parrello
 *
 */
public class SampleFileUtils {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SampleFileUtils.class);
    /** list of sample metadata objects */
    private List<SampleMeta> samples;
    /** pattern for removing repeat-number strings */
    private static final Pattern REPEAT_NUM = Pattern.compile("_rep\\d+");

    /**
     * Construct a list of samples from the specified input file.
     *
     * @param sampleFile	sample metadata file
     *
     * @throws IOException
     */
    public SampleFileUtils(File sampleFile) throws IOException {
        this.samples = new ArrayList<SampleMeta>(300);
        try (TabbedLineReader reader = new TabbedLineReader(sampleFile)) {
            for (TabbedLineReader.Line line : reader)
                this.samples.add(new SampleMeta(line));
        }
    }

    /**
     * Save the samples to the specified file.
     *
     * @param outFile	output file to which to save the samples
     */
    public void save(File outFile) throws IOException {
        try (PrintWriter writer = new PrintWriter(outFile)) {
            writer.println(SampleMeta.headers());
            for (SampleMeta sample : this.samples)
                writer.println(sample.toString());
        }
    }

    /**
     * Create a map of sample ID strings to sample metadata objects.
     */
    public Map<String, SampleMeta> getFullMap() {
        Map<String, SampleMeta> retVal = this.samples.stream().collect(Collectors.toMap(x ->x.getSampleId(),
                Function.identity(), (k,v) -> k, HashMap::new));
        return retVal;
    }

    /**
     * Create a map of likely sample ID strings to sample metadata objects.  This takes as input a set of
     * sample ID strings considered likely.  The repeat information is stripped off the strings and then
     * each stripped string is mapped to the sample metadata object containing the corresponding likely ID string.
     * This insures that when we parse the input files (which contain no repeat numbers) we find the correct sample.
     *
     * @param likelySet		set of strings representing the likely sample IDs
     */
    public Map<String, SampleMeta> getLikelyMap(Set<String> likelySet) {
        Map<String, SampleMeta> retVal = new HashMap<String, SampleMeta>((likelySet.size() * 4 + 2) / 3);
        for (SampleMeta sample : this.samples) {
            String sampleString = sample.getSampleId();
            if (likelySet.contains(sampleString)) {
                String genericString = RegExUtils.removeAll(sampleString, REPEAT_NUM);
                retVal.put(genericString, sample);
            }
        }
        return retVal;
    }

    /**
     * Update the sample metadata with optical density data from a matrix file.  The matrix file is tab-delimited
     * with no headers and a double-slash separator between sections.  The first section contains the sample IDs and
     * the second contains the corresponding optical densities.
     *
     * @param inFile		file containing the optical density matrices
     * @param timePoint		time point string
     * @param sampleMap		map of sample IDs to metadata objects
     *
     * @throws IOException
     */
    public void processDensityMatrixFile(File inFile, String timePoint, Map<String, SampleMeta> sampleMap) throws IOException {
        try (LineReader reader = new LineReader(inFile)) {
            // First we build the matrix of sample name strings.
            List<String[]> rows = new ArrayList<String[]>(12);
            for (String[] row : reader.new Section("//")) {
                // Each string in this row is an old-style strain ID.  We combine it with the time point to compute
                // the sample ID string.
                for (int i = 0; i < row.length; i++)
                    row[i] = SampleId.translate(row[i], timePoint).toString();
                rows.add(row);
            }
            // Now we use the sample strings to find the SampleMeta objects and plug in the optical density.
            int rowIdx = 0;
            for (String[] dataRow : reader.new Section(null)) {
                // Get the corresponding row of names.
                String[] row = rows.get(rowIdx);
                rowIdx++;
                // Each string in this row is an optical density.  If the corresponding sample ID is one in which
                // we are interested, we convert it to a number and store it.
                for (int i = 0; i < dataRow.length; i++) {
                    if (i < row.length) {
                        SampleMeta targetSample = sampleMap.get(row[i]);
                        if (targetSample == null)
                            log.debug("Sample {} in optical density matrix is not interesting.", row[i]);
                        else {
                            // Here we have a sample of interest.
                            double density = Double.valueOf(dataRow[i]);
                            targetSample.setDensity(density);
                        }
                    }
                }
            }
        }
    }

    /**
     * Update the sample metadata from a production data file.  The production data file is tab-delimited with
     * headers.  The first column contains old-style strain IDs.  The remaining columns contain production data
     * at different times.  The column headers in the remaining columns specify the time points.
     *
     * @param prodFile		production data file name
     * @param sampleMap		map of sample IDs to metadata objects
     *
     * @throws IOException
     */
    public void processProductionFile(File prodFile, Map<String, SampleMeta> sampleMap) throws IOException {
        try (TabbedLineReader reader = new TabbedLineReader(prodFile)) {
            // Get the headers:  these contain the time points.
            String[] times = reader.getLabels();
            // Loop through the data lines.
            for (TabbedLineReader.Line line : reader) {
                String strainId = line.get(0);
                // Loop through the columns.
                for (int i = 1; i < times.length; i++) {
                    String sampleStrain = SampleId.translate(strainId, times[i]).toString();
                    SampleMeta targetSample = sampleMap.get(sampleStrain);
                    if (targetSample == null)
                        log.debug("Sample {} in production file is not interesting.", sampleStrain);
                    else {
                        // Here we have a sample of interest.
                        double prod = line.getDouble(i);
                        targetSample.setProduction(prod);
                    }
                }
            }
        }
    }

    /**
     * @return the number of samples in this object
     */
    public int size() {
        return this.samples.size();
    }
}
