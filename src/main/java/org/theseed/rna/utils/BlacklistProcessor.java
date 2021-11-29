/**
 *
 */
package org.theseed.rna.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.BasePipeProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This command reads a file of feature-pair correlations and removes the ones between
 * features in a single group.
 *
 * The positional parameters are the name of the file containing the group definitions
 * and the name of the column containing the group IDs.  The file must be tab-delimited,
 * with headers, and the column name can also be a 1-based column index.
 *
 * The feature pairs are read from the standard input.  This should also be a tab-delimited
 * file with headers.  The default assumes the feature IDs are in the first two columns,
 * but this can be overridden.
 *
 * The output will be to the standard output.  It will include the header record plus
 * records for all the pairs where the two features are not in the same group.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file containing pairs (if not STDIN)
 * -o	output file for reduced file (if not STDOUT)
 *
 * --col1	index (1-based) or name of input file column containing first feature ID
 * --col2	index (1-based) or name of input file column containing second feature ID
 *
 * @author Bruce Parrello
 *
 */
public class BlacklistProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(BlacklistProcessor.class);
    /** map of feature IDs to group names */
    private Map<String, String> groupMap;
    /** index of the input column for the first feature ID */
    private int col1Idx;
    /** index of the input column for the second feature ID */
    private int col2Idx;

    // COMMAND-LINE OPTIONS

    /** index (1-based) or name of the first feature ID input column */
    @Option(name = "--col1", metaVar = "fid1", usage = "index (1-based) or name of the input column for the first feature ID")
    private String col1Name;

    /** index (1-based) or name of the second feature ID input column */
    @Option(name = "--col2", metaVar = "fid2", usage = "index (1-based) or name of the input column for the second feature ID")
    private String col2Name;

    /** name of the group input file */
    @Argument(index = 0, metaVar = "groupFile", usage = "name of the file containing the group definitions", required = true)
    private File groupFile;

    /** index (1-based) or name of the group ID column in the group file */
    @Argument(index = 1, metaVar = "groupCol", usage = "index (1-based) or name of the group file column containing the group ID", required = true)
    private String groupColName;

    @Override
    protected void setPipeDefaults() {
        this.col1Name = "1";
        this.col2Name = "2";
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
        // Verify the group file.
        if (! this.groupFile.canRead())
            throw new FileNotFoundException("Group input file " + this.groupFile + " is not found or unreadable.");
        // Read in the group map.
        try (TabbedLineReader groupStream = new TabbedLineReader(this.groupFile)) {
            // This will track the number of groups found.
            Set<String> groups = new HashSet<String>(500);
            this.groupMap = new HashMap<String, String>(1000);
            int groupIdx = groupStream.findField(this.groupColName);
            for (TabbedLineReader.Line line : groupStream) {
                String fid = line.get(0);
                String group = line.get(groupIdx);
                groups.add(group);
                this.groupMap.put(fid, group);
            }
            log.info("{} features found in {} groups.", this.groupMap.size(), groups.size());
        }
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
        // Get the column indices from the input file.
        this.col1Idx = inputStream.findField(this.col1Name);
        this.col2Idx = inputStream.findField(this.col2Name);
    }


    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // Write the header line to the output.
        writer.println(inputStream.header());
        // Loop through the input lines, keeping the ones with separated features.
        int count = 0;
        int kept = 0;
        for (TabbedLineReader.Line line : inputStream) {
            String operon1 = this.groupMap.get(line.get(this.col1Idx));
            boolean keep = (operon1 == null);
            if (! keep) {
                String operon2 = this.groupMap.get(line.get(this.col2Idx));
                keep = (operon2 == null || ! operon1.contentEquals(operon2));
            }
            if (keep) {
                // The features are in different operons, so we keep the line.
                writer.println(line.toString());
                kept++;
            }
            count++;
            if (count % 1000 == 0)
                log.info("{} lines processed, {} kept.", count, kept);
        }
        // All done.  Log the final summary.
        log.info("{} lines read, {} kept.", count, kept);
    }

}
