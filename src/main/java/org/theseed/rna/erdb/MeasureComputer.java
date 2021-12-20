/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.theseed.utils.ParseFailureException;;

/**
 * This is the base class for computing measurements of an RNA Seq expression data sample.
 * It is called by the loader to measurements for a sample, which are then queued for
 * loading.
 *
 * @author Bruce Parrello
 *
 */
public abstract class MeasureComputer {

    /**
     * This interface is used to retrieve measurement parameters from the controlling command processor.
     */
    public interface IParms {

        /**
         * @return the name of a tab-delimited file mapping sample IDs to measurements
         */
        public File getMeasureFile();

    }

    /**
     * This enum describes the types of measurement engines.
     */
    public static enum Type {
        THREONINE {
            @Override
            public MeasureComputer create(IParms processor) {
                return new ThreonineMeasureComputer(processor);
            }
        }, FILE {
            @Override
            public MeasureComputer create(IParms processor) throws ParseFailureException, IOException {
                return new FileMeasureComputer(processor);
            }
        };

        /**
         * @return a measurement computer for this type/
         *
         * @param processor		controlling command processor
         *
         * @throws ParseFailureException
         * @throws IOException
         */
        public abstract MeasureComputer create(IParms processor) throws ParseFailureException, IOException;
    }

    // FIELDS
    /** queue of measurements to load; note that only one measurement per type is allowed in here */
    private Set<MeasurementDesc> measures;
    /** ID of the current sample */
    private String sampleId;

    /**
     * Construct a blank, empty measurement collector.
     */
    public MeasureComputer() {
    }

    /**
     * Add a measurement to this collector.
     *
     * @param type			type of measurement
     * @param value			value measured
     */
    protected void addMeasurement(String type, double value) {
        // We only keep real values.
        if (Double.isFinite(value)) {
            MeasurementDesc measure = new MeasurementDesc(this.sampleId, type, value);
            this.measures.add(measure);
        }
    }

    /**
     * Add a list of measurements to this collector.
     *
     * @param measurements	collection of measurement descriptors
     */
    protected void addMeasurements(Collection<MeasurementDesc> measurements) {
        this.measures.addAll(measurements);
    }

    /**
     * Measure a sample and return its measurements.
     *
     * @param sampleId		ID of the sample being measured
     * @param data			RNA seq database containing the sample
     *
     * @return a collection of the measurements found
     */
    public Collection<MeasurementDesc> measureSample(String sample_id) {
        this.measures = new HashSet<MeasurementDesc>();
        this.sampleId = sample_id;
        this.getMeasurements(sample_id);
        // Hand off the measurements to the caller.
        var retVal = this.measures;
        this.measures = null;
        return retVal;
    }

    /**
     * Perform the measurements on the sample.
     *
     * @param sample_id		ID of the sample whose measurements are desired
     *
     * @return a collection of the measurements found
     */
    protected abstract void getMeasurements(String sample_id);

}
