/**
 *
 */
package org.theseed.samples;

import org.apache.commons.lang3.StringUtils;
import org.theseed.io.TabbedLineReader;

/**
 * This is a simple object that tracks the metadata information used to build the web data for an RNA sequence sample.  This
 * information is stored in the sampleMeta.tbl file used by the fpkmSummary command.
 *
 * @author Bruce Parrello
 *
 */
public class SampleMeta {

    // FIELDS
    /** new sample ID */
    private String sampleId;
    /** old sample ID */
    private String oldId;
    /** production mg/L */
    private double production;
    /** growth OD/600nm */
    private double density;
    /** TRUE if suspicious */
    private boolean suspicious;

    /**
     * Construct an empty sample object
     *
     * @param oldId		old-format sample ID
     * @param newId		new-format sample ID
     */
    public SampleMeta(String oldId, String newId) {
        this.oldId = oldId;
        this.sampleId = newId;
        this.production = Double.NaN;
        this.density = Double.NaN;
        this.suspicious = false;
    }

    /**
     * Construct a sample object from an input line.
     *
     * @param line		input line
     */
    public SampleMeta(TabbedLineReader.Line line) {
        this.sampleId = line.get(0);
        this.production = computeDouble(line.get(1));
        this.density = computeDouble(line.get(2));
        this.oldId = line.get(3);
        this.suspicious = line.getFlag(4);
    }

    /**
     * @return the production level
     */
    public double getProduction() {
        return this.production;
    }

    /**
     * Specify the production level.
     *
     * @param production 	the production to set
     */
    public void setProduction(double production) {
        this.production = production;
    }

    /**
     * @return the optical density (growth)
     */
    public double getDensity() {
        return this.density;
    }

    /**
     * Specify the optical density.
     *
     * @param density 	the density to set
     */
    public void setDensity(double density) {
        this.density = density;
    }

    /**
     * Store new production and density values if the current ones are unspecified.
     *
     * @param newGrowth		new density value
     * @param newProd		new production value
     */
    public void fillPerformanceData(double newGrowth, double newProd) {
        if (Double.isNaN(this.density))
            this.density = newGrowth;
        if (Double.isNaN(this.production))
            this.production = newProd;
    }

    /**
     * @return TRUE if this sample is suspicious
     */
    public boolean isSuspicious() {
        return this.suspicious;
    }

    /**
     * Specify whether or not this sample is suspicious.
     *
     * @param suspicious 	TRUE iff this sample is suspicious
     */
    public void setSuspicious(boolean suspicious) {
        this.suspicious = suspicious;
    }

    /**
     * @return the new-format sample ID
     */
    public String getSampleId() {
        return this.sampleId;
    }

    /**
     * @return the old-format sample ID
     */
    public String getOldId() {
        return this.oldId;
    }

    /**
     * @return the sampleMeta output line for this sample
     */
    public String toString() {
        String prodString = (Double.isNaN(this.production) ? "" : String.format("%4.0f", this.production));
        String densString = (Double.isNaN(this.density) ? "" : String.format("%6.2f", this.density));
        String flag = (this.suspicious ? "X" : "");
        return StringUtils.joinWith("\t", this.getSampleId(), prodString, densString, this.oldId, flag);
    }

    /**
     * @return a string as a double, converting the empty string into NaN
     *
     * @param string	input string to parse
     */
    public static double computeDouble(String string) {
        double retVal = Double.NaN;
        if (! string.isEmpty())
            retVal = Double.valueOf(string);
        return retVal;
    }

    /**
     * @return the recommended headers for the output file
     */
    public static String headers() {
        return "sample\tthr_mg/l\tOD_600nm\told_name\tsuspicious";
    }

}
