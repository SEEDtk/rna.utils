/**
 *
 */
package org.theseed.rna.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.excel.CustomWorkbook;
import org.theseed.genome.ClusterFeatureData;
import org.theseed.rna.RnaData;
import org.theseed.rna.RnaData.Row;

/**
 * This command creates spreadsheets for RNA databases in a single directory.  Each database is put in
 * a worksheet of the output spreadsheet.  For each feature in a sheet, it contains the feature
 * ID, its gene name, its operon name, its feature-cluster ID, its regulon ID, its iModulon IDs, and its
 * subsystem IDs.  Then the baseline, minimum, maximum, and standard deviation.  Finally, the
 * individual expression levels for each sample are displayed, with color-coding for distance from the baseline.
 *
 * The resulting sheet is overwhelmingly large, but it is presumed the user will hide and reveal
 * columns as needed.
 *
 * The positional parameters are the name of the input directory, the name of the output
 * spreadsheet, and the groups file (which should contain the subsystems and the clusters).
 *
 * A single file can be specified in lieu of a directory.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * @author Bruce Parrello
 *
 */
public class ClusterSheetProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(ClusterSheetProcessor.class);
    /** map of feature IDs to clustering data */
    private Map<String, ClusterFeatureData> clusterMap;
    /** array of input files */
    private File[] dataFiles;
    /** initial columns in each spreadsheet */
    private static final List<String> INIT_HEADERS = Arrays.asList("fid", "gene", "operon", "cluster",
            "regulon", "modulons", "subsystems", "baseline", "sdev", "min", "max");
    /** maximum display column width in spreadsheet */
    private static final int MAX_COL_WIDTH = 9000;

    // COMMAND-LINE OPTIONS

    /** input RNA database */
    @Argument(index = 0, metaVar = "inDir", usage = "RNA seq expression database", required = true)
    private File rnaDataDir;

    /** output spreadsheet file */
    @Argument(index = 1, metaVar = "outFile.xlsx", usage = "output spreadsheet", required = true)
    private File outFile;

    /** cluster groups file */
    @Argument(index = 2, metaVar = "gene.clusters.tbl", usage = "gene clustering table", required = true)
    private File groupFile;

    @Override
    protected void setDefaults() {
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        if (this.rnaDataDir.isDirectory()) {
            this.dataFiles = this.rnaDataDir.listFiles(new RnaData.FileFilter());
            log.info("{} RNA database files found in {}.", this.dataFiles.length, this.rnaDataDir);
        } else if (this.rnaDataDir.isFile()) {
            this.dataFiles = new File[] { this.rnaDataDir };
            log.info("Processing singleton RNA database in {}.", this.rnaDataDir);
        }
        if (! this.groupFile.canRead())
            throw new FileNotFoundException("Gene-cluster group file " + this.groupFile + " is not found or unreadable.");
        // Read in the cluster map.
        log.info("Reading gene-clustering data from {}.", this.groupFile);
        this.clusterMap = ClusterFeatureData.readMap(this.groupFile);
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Open the workbook for output.
        try (CustomWorkbook workbook = CustomWorkbook.create(this.outFile)) {
            // Set the maximum column size.
            workbook.setMaxWidth(MAX_COL_WIDTH);
            // Loop through the input files.
            for (File inFile : this.dataFiles) {
                log.info("Reading RNA database from {}.", inFile);
                RnaData data = RnaData.load(inFile);
                // Now we need to set up the headers.
                List<String> headers = new ArrayList<String>(INIT_HEADERS.size() + data.size());
                headers.addAll(INIT_HEADERS);
                data.getSamples().stream().map(x -> x.getName()).forEach(x -> headers.add(x));
                // Create a sheet for this database and put the headers in it.  Note that we
                // are construing the sheet as an Excel table.
                String sheetName = StringUtils.substringBefore(inFile.getName(), ".");
                workbook.addSheet(sheetName, true);
                workbook.setHeaders(headers);
                // The sheet is now ready.  We process one feature at a time, but we want them in
                // order.
                data.getRows().stream().sorted().forEach(x -> this.processRow(workbook, x));
                // Finish off the worksheet.
                workbook.autoSizeColumns();
                workbook.closeSheet();
            }
        }
    }

    /**
     * Enter the data for a single row into the workbook.  This includes the large number of supporting
     * items plus the expression levels themselves.
     *
     * @param workbook	workbook being updated
     * @param row		row to put into the workbook
     */
    private void processRow(CustomWorkbook workbook, Row row) {
        var feat = row.getFeat();
        var fid = feat.getId();
        log.debug("Processing row for {}.", fid);
        workbook.addRow();
        // Get the baseline for the row and the statistics.
        double baseline = feat.getBaseLine();
        DescriptiveStatistics stats = RnaData.getStats(row);
        // Get the feature's cluster data.
        ClusterFeatureData clData = this.clusterMap.get(fid);
        // Fill in the descriptive columns.  Note the functional assignment is a tooltip on the fid.
        workbook.storeCell(fid, null, clData.getFunction());  	// PATRIC feature ID
        workbook.storeCell(feat.getGene()); 					// gene name
        workbook.storeCell(feat.getOperon()); 					// operon
        workbook.storeCell(clData.getClusterId());				// cluster
        workbook.storeCell(feat.getAtomicRegulon());			// atomic regulon
        workbook.storeCell(StringUtils.join(feat.getiModulons(), ", "));
                                                                // modulons
        workbook.storeCell(StringUtils.join(clData.getSubsystems(), ", "));
                                                                // subsystems
        // Fill in the statistical columns.
        workbook.storeCell(baseline);							// baseline
        workbook.storeCell(stats.getStandardDeviation());		// standard deviation
        workbook.storeCell(stats.getMin());						// minimum
        workbook.storeCell(stats.getMax());						// maximum
        // Now we process the individual data columns, one per sample.  We need to know the
        // upper and lower bound of the normal expression range.
        double lowerBound = baseline / 2.0;
        double upperBound = baseline * 2.0;
        for (int i = 0; i < row.size(); i++) {
            if (! row.isGood(i))
                workbook.storeBlankCell();
            else {
                double w = row.getWeight(i).getWeight();
                workbook.storeCell(w, lowerBound, upperBound);
            }
        }
    }

}
