/**
 *
 */
package org.theseed.reports;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.rna.RnaData;
import org.theseed.rna.RnaFeatureData;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Hyperlink;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 * Here the report is written to an excel file.  This allows us to put links in and makes it possible for the consumer to do sorts
 * and other analytical transformations.
 *
 * @author Bruce Parrello
 *
 */
public class ExcelFpkmReporter extends FpkmReporter {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(ExcelFpkmReporter.class);
    /** current workbook */
    private Workbook workbook;
    /** current worksheet */
    private Sheet worksheet;
    /** saved output stream */
    private OutputStream outStream;
    /** input directory used to compute samstat links */
    private String inDir;
    /** next row number */
    private int rowNum;
    /** number of samples */
    private int nSamples;
    /** saved list of sample descriptors */
    private List<RnaData.JobData> samples;
    /** array of TPM totals */
    private double[] totals;
    /** current row */
    private org.apache.poi.ss.usermodel.Row ssRow;
    /** header style */
    private CellStyle headStyle;
    /** alert style */
    private CellStyle alertStyle;
    /** normal style */
    private CellStyle numStyle;
    /** high-expression style */
    private CellStyle highStyle;
    /** low-expression style */
    private CellStyle lowStyle;
    /** main sheet name */
    private String sheetName;
    /** controlling processor */
    private IParms processor;
    /** default column width */
    private static int DEFAULT_WIDTH = 10 * 256 + 128;
    /** function column width */
    private static int FUNCTION_WIDTH = 20 * 256;
    /** format for feature links */
    private static final String FEATURE_VIEW_LINK = "https://www.patricbrc.org/view/Feature/%s";
    /** index of first sample column */
    private static final int SAMP_COL_0 = 9;

    /**
     * Construct the reporter for the specified output stream and controlling processor.
     *
     * @param output		target output stream
     * @param processor		controlling processor
     */
    public ExcelFpkmReporter(OutputStream output, IParms processor) {
        // Save the output stream.
        this.outStream = output;
        // Save the PATRIC workspace input directory name and the worksheet name.
        this.inDir = processor.getInDir();
        this.sheetName = processor.getSheetName();
        // Save the processor so we can get other parms.
        this.processor = processor;
    }

    @Override
    protected void openReport(List<RnaData.JobData> actualSamples) {
        log.info("Creating workbook.");
        // Save the sample count and the sample array.
        this.nSamples = actualSamples.size();
        this.samples = actualSamples;
        // Create the workbook and the sheet.
        Workbook myWorkbook = new XSSFWorkbook();
        this.worksheet = myWorkbook.createSheet(this.sheetName);
        // Get a data formatter.
        DataFormat format = myWorkbook.createDataFormat();
        short fmt = format.getFormat("###0.0000");
        // Create the header style.
        this.headStyle = myWorkbook.createCellStyle();
        this.headStyle.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
        this.headStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        // Create the number style.
        this.numStyle = myWorkbook.createCellStyle();
        this.numStyle.setDataFormat(fmt);
        // Create the alert style.
        this.alertStyle = myWorkbook.createCellStyle();
        this.alertStyle.setFillForegroundColor(IndexedColors.YELLOW.getIndex());
        this.alertStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        // Create the level styles.
        this.highStyle = myWorkbook.createCellStyle();
        this.highStyle.setFillForegroundColor(IndexedColors.BRIGHT_GREEN.getIndex());
        this.highStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        this.lowStyle = myWorkbook.createCellStyle();
        this.lowStyle.setFillForegroundColor(IndexedColors.ROSE.getIndex());
        this.lowStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        // Create the totals array.
        this.totals = new double[this.nSamples];
        Arrays.fill(this.totals, 0.0);
        // Save the workbook so we know we're initialized.
        this.workbook = myWorkbook;
        // Now we add the column headers.
        this.rowNum = 0;
        this.addRow();
        this.setStyledCell(0, "peg_id", this.headStyle);
        this.setStyledCell(1, "gene", this.headStyle);
        this.setStyledCell(2, "bNumber", this.headStyle);
        this.setStyledCell(3, "function", this.headStyle);
        this.setStyledCell(4, "neighbor", this.headStyle);
        this.setStyledCell(5, "AR_num", this.headStyle);
        this.setStyledCell(6, "operon", this.headStyle);
        this.setStyledCell(7, "iModulons", this.headStyle);
        this.setStyledCell(8, "baseLine", this.headStyle);
        int colNum = SAMP_COL_0;
        // After the header columns, there is one column per sample.  Each is hyperlinked to its samstat page.
        for (RnaData.JobData sample : actualSamples) {
            Cell cell = this.setStyledCell(colNum, sample.getName(), headStyle);
            String url = this.getSamstatLink(sample.getName());
            this.setHref(cell, url);
            colNum++;
        }
        // Now we have the metadata rows.
        this.addRow();
        this.setStyledCell(0, "Old Name", this.headStyle);
        String[] nameValues = actualSamples.stream().map(x -> x.getOldName()).toArray(String[]::new);
        this.fillMetaRow(nameValues);
        this.addRow();
        this.setStyledCell(0, "Thr g/l", this.headStyle);
        double[] prodValues = actualSamples.stream().mapToDouble(x -> x.getProduction()).toArray();
        this.fillMetaRow(prodValues);
        this.addRow();
        this.setStyledCell(0, "OD", this.headStyle);
        double[] optValues = actualSamples.stream().mapToDouble(x -> x.getOpticalDensity()).toArray();
        this.fillMetaRow(optValues);
    }

    /**
     * Fill the current row with meta-data strings.
     *
     * @param values	array of meta-data values
     */
    private void fillMetaRow(String[] values) {
        int colNum = SAMP_COL_0;
        for (String v : values)
            this.setTextCell(colNum++, v);
    }

    /**
     * Fill the current row with meta-data numbers.
     *
     * @param values	array of meta-data values
     */
    private void fillMetaRow(double[] values) {
        int colNum = SAMP_COL_0;
        for (double v : values) {
            if (! Double.isNaN(v))
                this.setNumCell(colNum, v);
            colNum++;
        }
    }

    /**
     * Add a new row to the spreadsheet.
     */
    protected void addRow() {
        this.ssRow = this.worksheet.createRow(this.rowNum);
        this.rowNum++;
    }

    /**
     * Set the hyperlink for a cell.
     *
     * @param cell	cell to receive the hyperlink
     * @param url	URL to link from the cell
     */
    protected void setHref(Cell cell, String url) {
        final Hyperlink href = this.workbook.getCreationHelper().createHyperlink(HyperlinkType.URL);
        href.setAddress(url);
        cell.setHyperlink(href);
    }

    /**
     * Create a styled cell in the current row.
     *
     * @param i				column number of new cell
     * @param string		content of the cell
     * @param style			style to give the cell
     *
     * @return the created cell
     */
    private Cell setStyledCell(int i, String string, CellStyle style) {
        Cell retVal = setTextCell(i, string);
        retVal.setCellStyle(style);
        return retVal;
    }

    /**
     * Create a normal cell in the current row.
     *
     * @param i				column number of new cell
     * @param string		content of the cell
     *
     * @return the created cell
     */
    protected Cell setTextCell(int i, String string) {
        Cell retVal = this.ssRow.createCell(i);
        retVal.setCellValue(string);
        return retVal;
    }

    /**
     * @return the samstat link for a sample
     *
     * @param jobName	sample ID
     */
    private String getSamstatLink(String jobName) {
        return String.format("https://www.patricbrc.org/workspace%s/.%s_rna/Tuxedo_0_replicate1_%s_R1_001_ptrim.fq_%s_R2_001_ptrim.fq.bam.samstat.html",
                this.inDir, jobName, jobName, jobName);
    }

    /**
     * Link a cell to a PATRIC feature.
     *
     * @param cell	cell to link
     * @param feat	target feature for link (if NULL, no link will be generated)
     */
    public void setHref(Cell cell, RnaFeatureData feat) {
        if (feat != null) {
            try {
                String encodedFid = URLEncoder.encode(feat.getId(), StandardCharsets.UTF_8.toString());
                String url = String.format(FEATURE_VIEW_LINK, encodedFid);
                this.setHref(cell, url);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("UTF-8 encoding unsupported.");
            }
        }
    }


    @Override
    protected void writeRow(RnaData.Row row) {
        // Get the main feature and its function.
        RnaFeatureData feat = row.getFeat();
        String fid = feat.getId();
        double baseLine = feat.getBaseLine();
        double highMin = baseLine * 2.0;
        double lowMax = baseLine / 2.0;
        // Create the row and put in the heading cell.
        this.addRow();
        Cell cell = this.setStyledCell(0, fid, this.headStyle);
        this.setHref(cell, feat);
        this.setTextCell(1, feat.getGene());
        this.setTextCell(2, feat.getBNumber());
        this.setTextCell(3, feat.getFunction());
        // Process the neighbor.
        RnaFeatureData neighbor = row.getNeighbor();
        String neighborId = "";
        if (neighbor != null)
            neighborId = neighbor.getId();
        cell = this.setTextCell(4, neighborId);
        this.setHref(cell, neighbor);
        // Process the regulon data.
        this.setNumCell(5, feat.getAtomicRegulon());
        this.setTextCell(6, feat.getOperon());
        this.setTextCell(7, StringUtils.join(feat.getiModulons(), ','));
        // Store the baseline.
        this.setNumCell(8,  feat.getBaseLine());
        // Now we run through the weights.
        int colNum = SAMP_COL_0;
        for (int i = 0; i < this.nSamples; i++) {
            RnaData.Weight weight = row.getWeight(i);
            if (weight == null)
                this.setTextCell(colNum, "");
            else {
                double wVal = weight.getWeight();
                cell = this.setNumCell(colNum, wVal);
                // An inexact hit is colored pink.  A high expression is green, a low expression is red.
                if (! weight.isExactHit())
                    cell.setCellStyle(this.alertStyle);
                else if (wVal >= highMin)
                    cell.setCellStyle(this.highStyle);
                else if (wVal <= lowMax)
                    cell.setCellStyle(this.lowStyle);
                // Update the totals.
                this.totals[i] += weight.getWeight();
            }
            colNum++;
        }
    }

    /**
     * Create a cell with a numeric value.
     *
     * @param colNum	column number of the cell
     * @param num		number to put in the cell
     *
     * @return the cell created
     */
    private Cell setNumCell(int colNum, double num) {
        Cell retVal = this.ssRow.createCell(colNum);
        if (! Double.isNaN(num))
            retVal.setCellValue(num);
        return retVal;
    }

    @Override
    protected void closeReport() {
        // All done, write out the workbook.
        try {
            if (this.workbook != null) {
                // Freeze the headers.
                this.worksheet.createFreezePane(1, 1);
                // Fix the column widths.
                this.worksheet.autoSizeColumn(0);
                this.worksheet.autoSizeColumn(1);
                this.worksheet.autoSizeColumn(2);
                this.worksheet.setColumnWidth(3, FUNCTION_WIDTH);
                this.worksheet.autoSizeColumn(4);
                for (int i = SAMP_COL_0; i < this.nSamples + SAMP_COL_0; i++)
                    this.worksheet.setColumnWidth(i, DEFAULT_WIDTH);
                // Create the totals page.
                log.info("Building totals page.");
                this.worksheet = this.workbook.createSheet("Totals");
                this.rowNum = 0;
                // Add a header row.
                this.addRow();
                this.setStyledCell(0, "Sample", this.headStyle);
                this.setStyledCell(1, "Old Name", this.headStyle);
                this.setStyledCell(2, "Thr g/l", this.headStyle);
                this.setStyledCell(3, "OD", this.headStyle);
                this.setStyledCell(4, "TPM Total", this.headStyle);
                this.setStyledCell(5, "reads", this.headStyle);
                this.setStyledCell(6, "size", this.headStyle);
                this.setStyledCell(7, "pct_qual", this.headStyle);
                this.setStyledCell(8, "avg_read_len", this.headStyle);
                this.setStyledCell(9, "coverage", this.headStyle);
                this.setStyledCell(10, "pct_expressed", this.headStyle);
                // Create the data rows, one per sample.
                for (int i = 0; i < nSamples; i++) {
                    this.addRow();
                    RnaData.JobData sample = this.samples.get(i);
                    this.setStyledCell(0, sample.getName(), this.headStyle);
                    List<Cell> cells = new ArrayList<Cell>(4);
                    cells.add(this.setTextCell(1, sample.getOldName()));
                    cells.add(this.setNumCell(2, sample.getProduction()));
                    cells.add(this.setNumCell(3, sample.getOpticalDensity()));
                    cells.add(this.setNumCell(4, this.totals[i]));
                    cells.add(this.setNumCell(5, sample.getReadCount()));
                    cells.add(this.setNumCell(6, sample.getBaseCount()));
                    cells.add(this.setNumCell(7, sample.getQuality()));
                    cells.add(this.setNumCell(8, sample.getMeanReadLen()));
                    cells.add(this.setNumCell(9, sample.getCoverage(this.processor.getGenomeLen())));
                    cells.add(this.setNumCell(10, sample.getExpressedPercent()));
                    // Color the cells for a suspicious sample.
                    if (sample.isSuspicious())
                        cells.stream().forEach(x -> x.setCellStyle(this.alertStyle));
                }
                this.worksheet.autoSizeColumn(0);
                this.worksheet.autoSizeColumn(1);
                this.worksheet.autoSizeColumn(4);
                log.info("Writing workbook.");
                this.workbook.write(this.outStream);
            }
            this.outStream.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
