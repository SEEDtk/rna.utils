/**
 *
 */
package org.theseed.rna.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.cli.CopyTask;
import org.theseed.cli.DirEntry;
import org.theseed.cli.DirTask;
import org.theseed.io.TabbedLineReader;
import org.theseed.samples.SampleId;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This command reads a directory of local RNA files and copies them to an output directory in the PATRIC workspace.
 * It takes as input the name of the input directory, the name of the output directory, and the name of the workspace.
 * A log of processed files is kept in the input directory so that we can smoothly resume after failure.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --progress	name of the progress file in the input directory that tracks which files have been copied; the default is
 * 				"progress.txt"
 * --workDir	name of the work directory for temporary files; the default is "Temp" in the current directory
 * --test		no files will be copied; used for debugging
 *
 * @author Bruce Parrello
 *
 */
public class RnaCopyProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(RnaCopyProcessor.class);
   /** progress file stream */
    private PrintWriter progressStream;
    /** list of files in the input directory to copy */
    private Set<File> inFiles;
    /** map of sample numbers to sample IDs */
    private Map<String, SampleId> sampleMap;
    /** set of samples already seen */
    private Set<SampleId> seenSet;
    /** array of filename patters for input */
    private String[] FASTQ_PATTERNS = new String[] { "fastq" };
    /** pattern for getting the sample ID from a fastq file name */
    public final static Pattern SAMPLE_FASTQ_FILE = Pattern.compile("(\\S+)(_R[12]_\\d+\\.fastq)");

    // COMMAND-LINE OPTIONS

    /** progress file base name */
    @Option(name = "--progress", metaVar = "progress.tbl", usage = "file name (relative to input directory) of progress file")
    private String progressFileName;

    /** local input directory */
    @Argument(index = 0, metaVar = "inDir", usage = "local input directory", required = true)
    private File inDir;

    /** work directory for temporary files */
    @Option(name = "--workDir", metaVar = "workDir", usage = "work directory for temporary files")
    private File workDir;

    /** suppress actual file copy */
    @Option(name = "--test", usage = "if specified, the program will run, but the PATRIC workspace will not be updated")
    private boolean testMode;

    /** remote output directory path */
    @Argument(index = 1, metaVar = "outDir", usage = "output directory in PATRIC workspace", required = true)
    private String outDir;

    /** workspace name */
    @Argument(index = 2, metaVar = "user@patricbrc.org", usage = "PATRIC workspace name", required = true)
    private String workspace;

    @Override
    protected void setDefaults() {
        this.progressFileName = "progress.txt";
        this.workDir = new File(System.getProperty("user.dir"), "Temp");
        this.testMode = false;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Validate the input directory.
        if (! this.inDir.isDirectory())
            throw new FileNotFoundException("Input directory " + this.inDir + " is not found or is invalid.");
        // Insure we have a work directory.
        if (! this.workDir.isDirectory()) {
            log.info("Creating work directory {}.", this.workDir);
            FileUtils.forceMkdir(this.workDir);
            this.workDir.deleteOnExit();
        } else
            log.info("Temporary files will be stored in {}.", this.workDir);
        // Get the list of FASTQ files in the input directory.
        this.inFiles = new TreeSet<File>(FileUtils.listFiles(this.inDir, FASTQ_PATTERNS, false));
        // Create the sample map.
        this.sampleMap = new HashMap<String, SampleId>(135);
        // Verify the progress file.
        boolean oldFile = false;
        File progressFile = new File(this.inDir, this.progressFileName);
        if (progressFile.exists()) {
            log.info("Scanning existing progress file {}.", progressFile);
            scanOldProgress(progressFile);
            // Denote we are appending.
            oldFile = true;
        }
        // Open the progress file for appending with autoflush.
        FileOutputStream fileStream = new FileOutputStream(progressFile, oldFile);
        this.progressStream = new PrintWriter(fileStream, true);
        // If this is a new file, write the header.
        if (! oldFile)
            this.progressStream.println("original\tsample");
        return true;
    }

    /**
     * Read the old progress file and update the sample map and input file set accordingly.
     *
     * @param progressFile	name of the progress file
     *
     * @throws IOException
     */
    protected void scanOldProgress(File progressFile) throws IOException {
        // This will count the number of input files eliminated.
        int count = 0;
        // Loop through the old progress file.
        try (TabbedLineReader oldReader = new TabbedLineReader(progressFile)) {
            int originalField = oldReader.findField("original");
            int sampleField = oldReader.findField("sample");
            for (TabbedLineReader.Line line : oldReader) {
                // Save the sample mapping.
                File oldFile = new File(this.inDir, line.get(originalField));
                String num = SampleId.getSampleNumber(oldFile);
                SampleId sample = new SampleId(line.get(sampleField));
                this.sampleMap.put(num, sample);
                if (this.inFiles.remove(oldFile)) count++;
            }
            log.info("{} files in {} samples already processed.", count, this.sampleMap.size());
        }
    }

    @Override
    protected void runCommand() throws Exception {
        // Read the output directory and track all the files currently in place.
        this.scanOutputDirectory();
        // Get a copy task for copying the files.
        CopyTask copyTask = new CopyTask(this.workDir, this.workspace);
        // Loop through the input files.
        for (File inFile : this.inFiles) {
            // Compute the file type.
            Matcher m = SAMPLE_FASTQ_FILE.matcher(inFile.getName());
            if (! m.matches())
                log.info("Skipping non-read file {}.", inFile);
            else {
                String fileType = m.group(2);
                SampleId sampleId = SampleId.translate(inFile);
                if (sampleId == null)
                    log.info("Skipping invalid sample file {}.", inFile);
                else {
                    // Here we have a valid sample.  Get the sample number.
                    String num = SampleId.getSampleNumber(inFile);
                    SampleId seenSample = this.sampleMap.get(num);
                    if (seenSample == null) {
                        // Here the sample is a new one.  Make sure its ID is unique.
                        while (this.seenSet.contains(sampleId))
                            sampleId.increment();
                        this.sampleMap.put(num, sampleId);
                    } else {
                        // Here another file for the same sample has already been seen, so we use the previously
                        // assigned Id.
                        sampleId = seenSample;
                    }
                    // Form the target file name from the sample ID.
                    String targetFile = this.outDir + "/" + sampleId.toString() + fileType;
                    // Copy the file and write the progress.
                    log.info("Copying {} to {}.", inFile, targetFile);
                    if (! this.testMode)
                        copyTask.copyLocalFile(inFile.toString(), targetFile);
                    progressStream.println(inFile.getName() + "\t" + sampleId.toString());
                }
            }
        }
        // Close the progress file.
        progressStream.close();
        log.info("All done.");
    }

    /**
     * Scan the remote output directory and store all the samples we've already seen.
     */
    private void scanOutputDirectory() {
        // Read the output directory.
        DirTask dirTask = new DirTask(this.workDir, this.workspace);
        log.info("Scanning output directory {}.", this.outDir);
        List<DirEntry> outputFiles = dirTask.list(this.outDir);
        // Create the file-seen map.
        this.seenSet = new HashSet<SampleId>(((outputFiles.size() + 100) * 4 + 2) / 3);
        // Put all the files into it.
        int count = 0;
        for (DirEntry entry : outputFiles) {
            if (entry.getType() == DirEntry.Type.READS) {
                // Verify that this is a read file for a sample.
                Matcher m = SAMPLE_FASTQ_FILE.matcher(entry.getName());
                if (m.matches()) {
                    // Update the seen data for this file's sample.
                    SampleId sample = new SampleId(m.group(1));
                    this.seenSet.add(sample);
                    count++;
                }
            }
        }
        log.info("{} files seen so far in {} samples.", count, this.seenSet.size());
    }

}
