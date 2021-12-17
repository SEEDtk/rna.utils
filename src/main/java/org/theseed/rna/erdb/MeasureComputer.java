/**
 *
 */
package org.theseed.rna.erdb;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.theseed.rna.RnaData;
import org.theseed.rna.RnaData.JobData;;

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
        };

        /**
         * @return a measurement computer for this type/
         *
         * @param processor		controlling command processor
         */
        public abstract MeasureComputer create(IParms processor);
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
     * Measure a sample and return its measurements.
     *
     * @param sampleId		ID of the sample being measured
     * @param data			RNA seq database containing the sample
     *
     * @return a collection of the measurements found
     */
    public Collection<MeasurementDesc> measureSample(String sample_id, RnaData data) {
        this.measures = new HashSet<MeasurementDesc>();
        this.sampleId = sample_id;
        RnaData.JobData sampleData = data.getJob(sample_id);
        int jobIdx = data.getColIdx(sample_id);
        this.getMeasurements(data, jobIdx, sampleData);
        // Hand off the measurements to the caller.
        var retVal = this.measures;
        this.measures = null;
        return retVal;
    }

    /**
     * Perform the measurements on the sample.
     *
     * @param data			RNA SEQ database containing the sample
     * @param jobIdx		column index of the sample
     * @param sampleData	job data for the sample
     *
     * @return a collection of the measurements found
     */
    protected abstract void getMeasurements(RnaData data, int jobIdx, JobData sampleData);

}
