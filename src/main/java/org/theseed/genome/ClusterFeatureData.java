/**
 *
 */
package org.theseed.genome;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.TabbedLineReader;

/**
 * This object contains data about a feature loaded from the cluster GENOME report.  This includes the cluster ID,
 * the gene name, the subsystem list, and the functional assignment.
 *
 * @author Bruce Parrello
 *
 */
public class ClusterFeatureData {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(ClusterFeatureData.class);
    /** feature ID */
    private String fid;
    /** cluster ID */
    private String clusterId;
    /** gene name */
    private String gene;
    /** subsystem list */
    private String[] subsystems;
    /** functional assignment */
    private String function;

    /**
     * Construct a feature-data object from a cluster report input line.
     *
     * @param line		input line to process
     */
    public ClusterFeatureData(TabbedLineReader.Line line) {
        this.fid = line.get(1);
        this.clusterId = line.get(0);
        this.gene = line.get(2);
        this.subsystems = StringUtils.split(line.get(3), ',');
        this.function = line.get(4);
    }

    /**
     * @return the feature ID
     */
    public String getFid() {
        return this.fid;
    }

    /**
     * @return the cluster ID
     */
    public String getClusterId() {
        return this.clusterId;
    }

    /**
     * @return the gene name
     */
    public String getGene() {
        return this.gene;
    }

    /**
     * @return the array of subsystem IDs
     */
    public String[] getSubsystems() {
        return this.subsystems;
    }

    /**
     * @return the functional assignment
     */
    public String getFunction() {
        return this.function;
    }

    /**
     * Read a cluster report and return a map of feature IDs to feature-data objects.
     *
     * @param inFile	input file to read
     *
     * @return a map from feature IDs to the associated feature data
     *
     * @throws IOException
     */
    public static Map<String, ClusterFeatureData> readMap(File inFile) throws IOException {
        var retVal = new HashMap<String, ClusterFeatureData>(5000);
        try (TabbedLineReader inStream = new TabbedLineReader(inFile)) {
            inStream.stream().map(x -> new ClusterFeatureData(x)).forEach(x -> retVal.put(x.getFid(), x));
        }
        return retVal;
    }

}
