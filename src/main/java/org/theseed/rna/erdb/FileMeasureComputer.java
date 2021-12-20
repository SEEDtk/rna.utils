/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.ParseFailureException;

/**
 * This measurement computer gets the measurements from a tab-delimited file.  The first column
 * contains a sample ID, and the remaining columns contain measurement values, with the measurement
 * type described by the column heading.  A blank value is treated as no measurement.
 *
 * @author Bruce Parrello
 *
 */
public class FileMeasureComputer extends MeasureComputer {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FileMeasureComputer.class);
    /** map of samples to measurement descriptors */
    private Map<String, List<MeasurementDesc>> measureMap;

    /**
     * Read in the measurement file and create the measure map.
     *
     * @param processor		controlling command processor
     *
     * @throws ParseFailureException
     * @throws IOException
     */
    public FileMeasureComputer(IParms processor) throws ParseFailureException, IOException {
        File measureFile = processor.getMeasureFile();
        if (measureFile == null)
            throw new ParseFailureException("Measurement file is required for FILE-type measurements.");
        this.measureMap = new HashMap<String, List<MeasurementDesc>>(100);
        // Build the measure map from the file.
        try (TabbedLineReader inStream = new TabbedLineReader(measureFile)) {
            // The headers tell us what is being measured in each column.
            String[] headers = inStream.getLabels();
            final int n = headers.length;
            int count = 0;
            for (TabbedLineReader.Line line : inStream) {
                String sampleId = line.get(0);
                List<MeasurementDesc> list =
                        this.measureMap.computeIfAbsent(sampleId, x -> new ArrayList<MeasurementDesc>(n));
                for (int i = 1; i < n; i++) {
                    String measurement = line.get(i);
                    if (! StringUtils.isBlank(measurement)) {
                        list.add(new MeasurementDesc(sampleId, headers[i], Double.valueOf(measurement)));
                        count++;
                    }
                }
            }
            log.info("{} samples found in measurement file {}.  {} values stored.",
                    this.measureMap.size(), measureFile, count);
        }
    }

    /**
     * @return a list of the samples specified in the file
     */
    public Set<String> getSamples() {
        return this.measureMap.keySet();
    }

    @Override
    protected void getMeasurements(String sample_id) {
        List<MeasurementDesc> list = this.measureMap.get(sample_id);
        if (! list.isEmpty())
            this.addMeasurements(list);
    }

}
