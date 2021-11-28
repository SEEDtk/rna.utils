/**
 *
 */
package org.theseed.rna.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Feature;
import org.theseed.rna.BaselineComputer;
import org.theseed.rna.ExpressionConverter;
import org.theseed.rna.IBaselineParameters;
import org.theseed.rna.IBaselineProvider;
import org.theseed.rna.RnaData;
import org.theseed.rna.RnaFeatureData;
import org.theseed.rna.TriageExpressionConverter;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This command will create a file describing the correlation between genes in an RNA database.  Two distance measures
 * are produced for each pair of genes-- the pearson coefficient of the expression levels and the percent of time
 * the two genes triage to the same value.  The intent is to provide a database for analyzing co-expression.
 *
 * The positional parameter is the name of the RNA database.  The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	output file (if not STDOUT)
 *
 * --baseline	method for computing triage baseline (TRIMEAN, SAMPLE, FILE)
 * --baseId		ID of the base sample for a SAMPLE baseline
 * --baseFile	name of file containing baseline data for a FILE baseline
 * --minCorr	minimum absolute value of pearson correlation for output
 *
 * @author Bruce Parrello
 *
 */
public class RnaCorrelationProcessor extends BaseProcessor implements IBaselineProvider, IBaselineParameters {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(RnaCorrelationProcessor.class);
    /** RNA database */
    private RnaData data;
    /** baseline computer */
    private BaselineComputer baselineComputer;
    /** triage array for each feature */
    private Map<String, double[]> triageMap;
    /** expression array for each feature */
    private Map<String, double[]> levelMap;
    /** output file stream */
    private OutputStream outStream;
    /** expression triage converter */
    private ExpressionConverter converter;
    /** regression calculator */
    private SimpleRegression pComputer = new SimpleRegression();

    // COMMAND-LINE OPTIONS

    /** output file (if not STDOUT) */
    @Option(name = "-o", aliases = { "--output" }, metaVar = "outFile.tbl", usage = "output file (if not STDOUT)")
    private File outFile;

    /** method for computing baseline for triage output */
    @Option(name = "--baseline", usage = "method for computing triage baseline")
    private BaselineComputer.Type baselineType;

    /** file containing baseline values */
    @Option(name = "--baseFile", usage = "file containing baseline values for triage type FILE output")
    private File baseFile;

    /** ID of sample containing baseline values */
    @Option(name = "--baseId", usage = "sample containing baseline values for triage type SAMPLE output")
    private String baseSampleId;

    /** minimum correlation value for output */
    @Option(name = "--minCorr", usage = "minimum absolute value of pearson correlation for output")
    private double minCorr;

    /** file containing the RNA database */
    @Argument(index = 0, metaVar = "rnaData.ser", usage = "RNA database file")
    private File rnaFile;


    @Override
    protected void setDefaults() {
        this.baselineType = BaselineComputer.Type.TRIMEAN;
        this.baseFile = null;
        this.baseSampleId = null;
        this.outFile = null;
        this.minCorr = 0.0;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Insure we have the RNA database.
        if (! this.rnaFile.canRead())
            throw new FileNotFoundException("RNA database file " + this.rnaFile + " not found or unreadable.");
        // Verify the output file.
        if (this.outFile == null) {
            log.info("Output will be to the standard output.");
            this.outStream = System.out;
        } else {
            log.info("Output will be to {}.", this.outFile);
            this.outStream = new FileOutputStream(this.outFile);
        }
        // Verify the minimum correlation.
        if (this.minCorr > 1.0 || this.minCorr < 0.0)
            throw new ParseFailureException("Minimum correlation value must be between 0.0 and 1.0.");
        // Verify the baseline data and create the computer.
        this.baselineComputer = BaselineComputer.validateAndCreate(this, this.baselineType);
        this.converter = new TriageExpressionConverter(this);
        // Load the RNA database.
        log.info("Loading RNA database from {}.", this.rnaFile);
        try {
            this.data = RnaData.load(this.rnaFile);
        } catch (ClassNotFoundException e) {
            // Convert this to an IO error. Almost always, it's an incompatible file.
            throw new IOException("Incompatible RNA data file " + this.rnaFile + ": " + e.toString());
        }
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        try {
            // Create the triage and expression maps.
            this.createRowMaps();
            // Get an array of all the feature rows.  We will do a two-level iteration through it to compute the pairs.
            RnaData.Row[] rowList = this.data.getRows().toArray(new RnaData.Row[this.data.height()]);
            // Sort the array for convenience.
            Arrays.sort(rowList);
            // Start the output.
            try (PrintWriter writer = new PrintWriter(this.outStream)) {
                writer.println("key\tfid1\tgene_name1\tfid2\tgene_name2\tpearson\tmatch");
                // Now loop through the row array.  For each feature, we perform the two comparisons between it and every
                // subsequent feature.
                for (int i = 0; i < rowList.length; i++) {
                    RnaFeatureData iFeat = rowList[i].getFeat();
                    String iFid = iFeat.getId();
                    log.info("Computing correlations for {} ({} of {}).", iFid, i, rowList.length);
                    double[] iTriage = this.triageMap.get(iFid);
                    double[] iLevels = this.triageMap.get(iFid);
                    for (int j = i + 1; j < rowList.length; j++) {
                        RnaFeatureData jFeat = rowList[j].getFeat();
                        String jFid = jFeat.getId();
                        // Compute the correlations.
                        double pc = getPearson(iLevels, this.levelMap.get(jFid));
                        // Only proceed if it is big enough to output.
                        if (Math.abs(pc) >= this.minCorr) {
                            double matchPct = this.getPercent(iTriage, this.triageMap.get(jFid), pc);
                            String key = Feature.pairKey(iFid, jFid);
                            // Write the data.
                            writer.format("%s\t%s\t%s\t%s\t%s\t%8.4f\t%8.2f%n", key, iFid, iFeat.getGene(),
                                    jFid, jFeat.getGene(), pc, matchPct);
                        }
                    }
                }
            }
        } finally {
            if (this.outFile != null)
                this.outStream.close();
        }
    }


/**
    * Create a map from each feature ID to (1) an array of -1,0,+1 values representing whether the expression value
    * is above or below the baseline and (2) an array of actual expression values.  Missing values are stored as NaN.
    */
   private void createRowMaps() {
       this.triageMap = new HashMap<String, double[]>((this.data.height() * 4 + 2) / 3);
       this.levelMap = new HashMap<String, double[]>((this.data.height() * 4 + 2) / 3);
       // Get the number of columns in the RNA database.
       int nCols = this.data.size();
       log.info("Computing triage levels for {} features.", this.data.height());
       // Loop through the rows, doing the triage.
       for (RnaData.Row row : this.data) {
           String fid = row.getFeat().getId();
           this.converter.analyzeRow(row);
           // Loop through the weights, converting them to expression levels.
           double[] triages = new double[nCols];
           double[] levels = new double[nCols];
           for (int i = 0; i < nCols; i++) {
               RnaData.Weight weight = row.getWeight(i);
               double wVal = Double.NaN;
               if (weight != null && weight.isExactHit())
                    wVal = weight.getWeight();
               if (Double.isFinite(wVal)) {
                   triages[i] = this.converter.convert(wVal);
                   levels[i] = wVal;
               } else {
                   triages[i] = Double.NaN;
                   levels[i] = Double.NaN;
               }
           }
           this.triageMap.put(fid, triages);
           this.levelMap.put(fid, levels);
       }
   }


    /**
     * @return the percent match between two triage arrays
     *
     * @param iTriage	first array
     * @param jTriage	second array
     * @param dir		if positive, an exact match is indicated; if negative, a negative match is indicated
     */
    private double getPercent(double[] iTriage, double[] jTriage, double dir) {
        int count = 0;
        int total = 0;
        double retVal = 0.0;
        double dir1 = (dir < 0.0 ? -1.0 : 1.0);
        for (int i = 0; i < iTriage.length; i++) {
            // Only look at samples for which the feature has a value in both.
            if (Double.isFinite(iTriage[i]) && Double.isFinite(jTriage[i])) {
                total++;
                if (iTriage[i] == dir1 * jTriage[i]) count++;
            }
        }
        if (count > 0)
            retVal = (count * 100.0) / total;
        return retVal;
    }

    /**
     * Compute the pearson correlation between two rows of expression data.  This is complicated because we are
     * only doing it for positions where both rows have a value.
     *
     * @param iRow		first feature's row
     * @param jRow		second feature's row
     *
     * @return the pearson coefficient of the expression values
     */
    protected double getPearson(double[] iRow, double[] jRow) {
        double retVal = 0.0;
        // Determine the indices where we have both values.
        int[] indices = IntStream.range(0, iRow.length).filter(i -> (Double.isFinite(iRow[i]) && Double.isFinite(jRow[i])))
                .toArray();
        if (indices.length >= 2) {
            this.pComputer.clear();
            for (int i0 = 0; i0 < indices.length; i0++) {
                int i = indices[i0];
                this.pComputer.addData(iRow[i], jRow[i]);
            }
            // Finally, the Pearson coefficient.
            retVal = this.pComputer.getR();
        }
        return retVal;
    }

    @Override
    public double getBaseline(RnaData.Row row) {
        return this.baselineComputer.getBaseline(row);
    }

    @Override
    public File getFile() {
        return this.baseFile;
    }

    @Override
    public String getSample() {
        return this.baseSampleId;
    }

    @Override
    public RnaData getData() {
        return this.data;
    }

}
