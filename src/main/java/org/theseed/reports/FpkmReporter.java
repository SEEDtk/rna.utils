/**
 *
 */
package org.theseed.reports;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Feature;
import org.theseed.io.LineReader;
import org.theseed.io.TabbedLineReader;
import org.theseed.rna.FileBaselineComputer;
import org.theseed.rna.RnaData;
import org.theseed.rna.RnaData.JobData;
import org.theseed.samples.SampleMeta;
import org.theseed.rna.RnaFeatureData;

/**
 * This is the base class for the TPM summary report.  This class is responsible for converting the columnar inputs to
 * rows.  That is, the data comes in by features within sample, but we want to write samples within feature.
 *
 * @author Bruce Parrello
 *
 */
public abstract class FpkmReporter implements AutoCloseable {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FpkmReporter.class);
    /** parsing pattern for samstat processing data (reads, size, creation) */
    protected static final Pattern PROCESSING_PATTERN = Pattern.compile("<p>(\\d+) reads, size:(\\d+) bytes, created (\\S+)");
    /** parsing pattern for samstat quality data (percent >= 30% good) */
    protected static final Pattern QUALITY_PATTERN = Pattern.compile("MAPQ >= 30</td> <td>\\d+\\.\\d+</td> <td>(\\d+\\.\\d+)");
    /** parsing pattern for TPM file name */
    protected static final Pattern TPM_PATTERN = Pattern.compile("(.+)_genes.fpkm");
    /** repository of collected data */
    private RnaData data;
    /** name of the current sample */
    private String jobName;


    /**
     * Enumeration of report formats
     */
    public static enum Type {
        TEXT, EXCEL, CSV, QUALITY;

        public FpkmReporter create(OutputStream output, IParms processor) {
            FpkmReporter retVal = null;
            switch (this) {
            case TEXT :
                retVal = new TextFpkmReporter.Tab(output, processor);
                break;
            case EXCEL:
                retVal = new ExcelFpkmReporter(output, processor);
                break;
            case CSV:
                retVal = new TextFpkmReporter.CSV(output, processor);
                break;
            case QUALITY:
                retVal = new QualityFpkmReporter(output, processor);
                break;
            }
            return retVal;
        }
    }


    /**
     * interface for retrieving parameters from the controlling processor.
     */
    public interface IParms {

        /**
         * @return the name of the input directory containing all the sample runs
         */
        public String getInDir();

        /**
         * @return the name to give to the main sheet
         */
        public String getSheetName();

        /**
         * @return the genome length
         */
        public int getGenomeLen();

    }


    /**
     * Initialize the report.
     */
    public void startReport() { }

    /**
     * Begin processing a single sample.
     *
     * @param jobName	sample name
     *
     * @return TRUE if the sample is valid, else FALSE
     */
    public boolean startJob(String jobName) {
        Integer jobIdx = this.data.findColIdx(jobName);
        boolean retVal = (jobIdx != null);
        if (retVal)
            // Save the sample name.
            this.jobName = jobName;
        return retVal;
    }

    /**
     * Record a hit.
     *
     * @param feat		feature hit
     * @param exactHit	TRUE if the hit was detected by the alignment
     * @param neighbor	neighboring feature
     * @param weight	weight of the hit
     */
    public void recordHit(Feature feat, boolean exactHit, Feature neighbor, double weight) {
        // Get the row for this feature.
        RnaData.Row fRow = this.data.getRow(feat, neighbor);
        // Add this weight.
        fRow.store(this.jobName, exactHit, weight);
    }

    /**
     * Terminate the report.  This performs final updates and actually produces the output.
     */
    public void endReport() {
        // First, we need to update the quality data.
        this.data.updateQuality();
        // Now, start the report.
        this.openReport(this.data.getSamples());
        // Sort the rows by location.
        SortedSet<RnaData.Row> rows = new TreeSet<RnaData.Row>(this.data.getRows());
        // Write them out.
        for (RnaData.Row row : rows)
            this.writeRow(row);
    }

    /**
     * Initialize the output.
     *
     * @param list		list of sample descriptors
     */
    protected abstract void openReport(List<JobData> list);

    /**
     * Write the data for a single row.
     *
     * @param row	row of weights to write for a specific feature.
     */
    protected abstract void writeRow(RnaData.Row row);

    /**
     * Finish the report and flush the output.
     */
    protected abstract void closeReport();

    @Override
    public void close() {
        this.closeReport();
    }

    /**
     * Read the meta-data file to get the JobData objects.
     *
     * @param in			input stream for meta-data file
     * @param abridgeFlag 	if specified, suspicious samples will be skipped
     *
     * @throws IOException
     */
    public void readMeta(InputStream in, boolean abridgeFlag) throws IOException {
        this.data = new RnaData();
        try (TabbedLineReader reader = new TabbedLineReader(in)) {
            for (TabbedLineReader.Line line : reader) {
                SampleMeta sampleMeta = new SampleMeta(line);
                if (! sampleMeta.isSuspicious() || ! abridgeFlag)
                    this.addJob(sampleMeta);
            }
        }
        log.info("{} samples found in meta-data file.", this.data.size());
    }

    /**
     * Add a new job from a sample metadata descriptor.
     *
     * @param sampleMeta	sample metadata descriptor
     */
    private void addJob(SampleMeta sampleMeta) {
        this.data.addJob(sampleMeta.getSampleId(), sampleMeta.getProduction() / 1000.0,
                sampleMeta.getDensity(), sampleMeta.getOldId(),
                sampleMeta.isSuspicious());
    }

    /**
     * Create JobData objects for all the specified files.
     *
     * @param files			array of tracking file names
     */
    public void createMeta(File[] fpkmFiles) {
        this.data = new RnaData();
        for (File fpkmFile : fpkmFiles) {
            Matcher m = TPM_PATTERN.matcher(fpkmFile.getName());
            if (m.matches()) {
                String sampleId = m.group(1);
                SampleMeta sampleMeta = new SampleMeta(sampleId, sampleId);
                this.addJob(sampleMeta);
            }
        }
    }

    /**
     * Read the samstat file to set the job quality/history data.
     *
     * @param jobName		name of the relevant job
     * @param samstatFile	samStat HTML file containing the quality and history data
     *
     * @throws IOException
     */
    public void readSamStat(String jobName, File samStatFile) throws IOException {
        // Find the job descriptor.
        RnaData.JobData job = this.data.getJob(jobName);
        // Read the samstat file.
        String htmlString = StringUtils.join(LineReader.readList(samStatFile), " ");
        // Get the processing data.
        Matcher m = PROCESSING_PATTERN.matcher(htmlString);
        if (! m.find())
            log.warn("WARNING: no creation data for {}.", jobName);
        else {
            job.setReadCount(Integer.valueOf(m.group(1)));
            job.setBaseCount(Long.valueOf(m.group(2)));
            job.setProcessingDate(LocalDate.parse(m.group(3)));
        }
        // Get the quality percentage.
        m = QUALITY_PATTERN.matcher(htmlString);
        if (! m.find())
            log.warn("WARNING: no quality percentage for {}.", jobName);
        else
            job.setQuality(Double.valueOf(m.group(1)));
    }

    /**
     * Save the accumulated RNA data in binary format
     *
     * @param saveFile		proposed save file
     *
     * @throws IOException
     */
    public void saveBinary(File saveFile) throws IOException {
        log.info("Saving binary output to {}.", saveFile);
        this.data.save(saveFile);
    }

    /**
     * Normalize the data accumulated for this report.
     */
    public void normalize() {
        this.data.normalize();
    }

    /**
     * Compute the baseline value for each feature and write the data to
     * a file for use in computing triage values.
     *
     * @param baseFile	output file name
     *
     * @throws IOException
     */
    public void saveBaseline(File baseFile) throws IOException {
        SortedMap<String, Double> baselines = this.data.getBaselines();
        try (PrintWriter writer = new PrintWriter(baseFile)) {
            writer.println("fid\tbaseline");
            for (Map.Entry<String, Double> entry : baselines.entrySet()) {
                String val = entry.getValue().toString();
                writer.println(entry.getKey() + "\t" + val);
            }
        }

    }

    /**
     * Read the regulon/modulon data from a regulon definition file.
     *
     * @param regulonFile	regulon definition file
     *
     * @throws IOException
     */
    public void readRegulons(File regulonFile) throws IOException {
        try (TabbedLineReader regStream = new TabbedLineReader(regulonFile)) {
            log.info("Reading regulon data from {}.", regulonFile);
            for (TabbedLineReader.Line line : regStream) {
                String fid = line.get(0);
                this.data.storeRegulonData(fid, line.getInt(2), line.get(1), line.get(3));
            }
        }
    }

    /**
     * Read the baseline data from a file and store it in the feature data for this RNA database.
     *
     * @param baseInFile	input file containing baseline data
     */
    public void loadBaseline(File baseInFile) {
        // Load in the baseline levels.
        FileBaselineComputer baseComputer = new FileBaselineComputer(baseInFile);
        // Loop through the features.
        for (RnaData.Row row : this.data) {
            RnaFeatureData feat = row.getFeat();
            double base = baseComputer.getBaseline(row);
            feat.setBaseLine(base);
        }

    }

    /**
     * Set the baseline expression level for each feature based on the trimean of all the
     * expression data in the database.
     */
    public void setBaseline() {
        Map<String, Double> baselines = this.data.getBaselines();
        // Loop through the features.
        for (RnaData.Row row : this.data) {
            RnaFeatureData feat = row.getFeat();
            double base = baselines.getOrDefault(feat.getId(), 0.0);
            feat.setBaseLine(base);
        }
    }

}
