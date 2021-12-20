/**
 *
 */
package org.theseed.rna.erdb;

import org.theseed.samples.SampleId;

/**
 * This is a measurement processor for RNA Seq samples generated for the threonine project.
 * The measurements involve the structure of the sample itself, based on the sample name.
 *
 * @author Bruce Parrello
 *
 */
public class ThreonineMeasureComputer extends MeasureComputer {

    /**
     * Construct a threonine measurement computer.
     *
     * @param processor		controlling command processor
     */
    public ThreonineMeasureComputer(IParms processor) {
        super();
    }

    @Override
    protected void getMeasurements(String sample_id) {
        // Create a sample object.
        SampleId sampleId = new SampleId(sample_id);
        // Parse the sample properties.
        this.addMeasurement("time", sampleId.getTimePoint());
        double value = (sampleId.isIPTG() ? 1.0 : 0.0);
           this.addMeasurement("iptg", value);
        this.addMeasurement(sampleId.getFragment(SampleId.STRAIN_COL), 1.0);
        String operon = sampleId.getFragment(SampleId.OPERON_COL);
        if (! operon.contentEquals("0"))
            this.addMeasurement("throp_" + operon, 1.0);
        for (String insert : sampleId.getInserts())
            this.addMeasurement("I" + insert, 1.0);
        for (String delete : sampleId.getDeletes())
            this.addMeasurement(delete, 1.0);
        // Finally, get the quantitative measurements.
    }

}
