/**
 *
 */
package org.theseed.rna.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.samples.SampleMeta;

/**
 * This command updates the sampleMeta.tbl file with new samples.  The "rna.production.tbl" file is used to get production and growth
 * data and the suspicion flag, and the "progress.txt" file is used to get the old and new names for the new samples.  If a sample
 * already exists, its sampleMeta record will be updated.
 *
 * The basic strategy is to build a descriptor for each sample and then write it out at the end.  Each of the input files is read
 * individually and the samples updated accordingly.
 *
 * The positional parameter is the name of the directory containing the files.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	show more detailed progress messages
 *
 * --prod	if specified, the name of a big production table; the growth and production data will be copied for samples that do
 * 			not already have it
 *
 * @author Bruce Parrello
 *
 */
public class SampleMetaProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SampleMetaProcessor.class);
    /** map of sample IDs to meta-data descriptors */
    private Map<String, SampleMeta> sampleMap;
    /** map of old IDs to new IDs */
    private Map<String, String> idMap;
    /** name of output file */
    private File outFile;
    /** name of progress file */
    private File progressFile;
    /** name of production file */
    private File prodFile;
    /** list of rep IDs to try */
    private static final String[] REP_SUFFIXES = new String[] { "", "_rep0", "_rep1", "rep2", "_rep3" };

    // COMMAND-LINE OPTIONS

    /** optional production file */
    @Option(name = "--prod", metaVar = "big_production_table.tsv", usage = "big production table containing production and growth data")
    private File bigProdFile;

    /** name of the directory containing all the relevant files */
    @Argument(index = 0, metaVar = "dataDir", usage = "directory containing all the files")
    private File dataDir;

    @Override
    protected void setDefaults() {
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        if (! this.dataDir.isDirectory())
            throw new FileNotFoundException("Data directory " + this.dataDir + " is not found or invalid.");
        this.outFile = new File(this.dataDir, "sampleMeta.tbl");
        if (! this.outFile.canRead())
            throw new FileNotFoundException("Sample input/output file " + this.outFile + " is not found or unreadable.");
        this.progressFile = new File(this.dataDir, "progress.txt");
        if (! this.progressFile.canRead())
            throw new FileNotFoundException("Copy progress file " + this.progressFile + " is not found or unreadable.");
        this.prodFile = new File(this.dataDir, "rna.production.tbl");
        if (! this.prodFile.canRead())
            throw new FileNotFoundException("Production/growth file " + this.prodFile + " is not found or unreadable.");
        if (this.bigProdFile != null && ! this.bigProdFile.canRead())
            throw new FileNotFoundException("Big production table file " + this.bigProdFile + " is not found or unreadable.");
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Create the maps.
        this.idMap = new HashMap<String, String>(300);
        this.sampleMap = new TreeMap<String, SampleMeta>();
        // Read the current sample file.
        log.info("Reading old samples from {}.", this.outFile);
        try (TabbedLineReader sampleReader = new TabbedLineReader(this.outFile)) {
            for (TabbedLineReader.Line line : sampleReader) {
                SampleMeta sampleMeta = new SampleMeta(line);
                this.sampleMap.put(sampleMeta.getSampleId(), sampleMeta);
                this.idMap.put(sampleMeta.getOldId(), sampleMeta.getSampleId());
            }
        }
        log.info("{} old samples read from file.", this.sampleMap.size());
        // Read the progress file to get the list of new samples.
        log.info("Reading new samples from {}.", this.progressFile);
        try (TabbedLineReader progressReader = new TabbedLineReader(this.progressFile)) {
            int count = 0;
            int found = 0;
            for (TabbedLineReader.Line line : progressReader) {
                // The old ID needs to be parsed out of the file name.
                Matcher m = RnaCopyProcessor.SAMPLE_FASTQ_FILE.matcher(line.get(0));
                if (m.matches()) {
                    String oldId = m.group(1);
                    count++;
                    // Connect this old ID with the appropriate sample ID.  We only do this
                    // the first time we see a sample.
                    String sampleId = line.get(1);
                    if (! this.sampleMap.containsKey(sampleId)) {
                        SampleMeta sampleMeta = new SampleMeta(oldId, sampleId);
                        this.sampleMap.put(sampleId, sampleMeta);
                        this.idMap.put(oldId, sampleId);
                        found++;
                    }
                }
            }
            log.info("{} new samples found in {} records.", found, count);
        }
        // Now, we read the production file to get the production and growth numbers.
        log.info("Reading production and growth data from {}.", this.prodFile);
        try (TabbedLineReader prodReader = new TabbedLineReader(this.prodFile)) {
            int count = 0;
            for (TabbedLineReader.Line line : prodReader) {
                String oldId = line.get(0);
                String sampleId = this.idMap.get(oldId);
                if (sampleId == null)
                    log.warn("Old sample ID {} not found,", oldId);
                else {
                    SampleMeta sampleMeta = this.sampleMap.get(sampleId);
                    double production = SampleMeta.computeDouble(line.get(1));
                    double density = SampleMeta.computeDouble(line.get(2));
                    sampleMeta.setProduction(production);
                    sampleMeta.setDensity(density);
                    sampleMeta.setSuspicious(line.getFlag((3)));
                    count++;
                }
            }
            log.info("{} production values updated.", count);
        }
        // Finally, we read the big production table to get defaults.
        if (this.bigProdFile != null) {
            log.info("Reading backup production and growth data from {}.", this.bigProdFile);
            try (TabbedLineReader bigProdReader = new TabbedLineReader(this.bigProdFile)) {
                int count = 0;
                int idCol = bigProdReader.findField("sample");
                int growthCol = bigProdReader.findField("growth");
                int prodCol = bigProdReader.findField("thr_production");
                for (TabbedLineReader.Line line : bigProdReader) {
                    String sampleId = line.get(idCol);
                    for (String repSuffix : REP_SUFFIXES) {
                        SampleMeta sampleMeta = this.sampleMap.get(sampleId + repSuffix);
                        if (sampleMeta != null) {
                            double growth = line.getDouble(growthCol);
                            // Note we have to scale from g/L to mg/L for production.
                            double prod = line.getDouble(prodCol) * 1000;
                            sampleMeta.fillPerformanceData(growth, prod);
                            count++;
                        }
                    }
                }
                log.info("{} growth/production defaults found.", count);
            }
        }
        // Everything is ready.  Backup the output file.
        FileUtils.copyFile(this.outFile, new File(this.dataDir, "sampleMeta.bak.tbl"));
        // Now write the output.
        log.info("Writing output.  {} samples available.", this.sampleMap.size());
        try (PrintWriter writer = new PrintWriter(this.outFile)) {
            writer.println(SampleMeta.headers());
            for (SampleMeta sampleMeta : this.sampleMap.values())
                writer.println(sampleMeta.toString());
        }
        log.info("All done.");
    }

}
