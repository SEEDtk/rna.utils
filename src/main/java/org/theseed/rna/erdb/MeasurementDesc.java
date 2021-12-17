/**
 *
 */
package org.theseed.rna.erdb;

import java.sql.SQLException;

import org.theseed.java.erdb.DbLoader;

/**
 * This class describes a measurement descriptor.  These are used when loading an RNA Seq database to queue
 * measurements for later processing.
 *
 * @author Bruce Parrello
 *
 */
public class MeasurementDesc {

    // FIELDS

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
