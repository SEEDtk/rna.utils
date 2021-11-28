/**
 *
 */
package org.theseed.rna.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.rna.RnaData;
import org.theseed.rna.RnaFeatureData;
import org.theseed.utils.BaseReportProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This command reads an RNA Seq database and writes out a table of group IDs by Blattner number.
 *
 * The positional parameter is the name of the RNA Seq database.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	name of the output file (if not STDOUT)
 *
 * @author Bruce Parrello
 *
 */
public class BlattnerProcessor extends BaseReportProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(BlattnerProcessor.class);
    /** RNA Seq database */
    private List<RnaFeatureData> feats;

    // COMMAND-LINE OPTIONS

    /** RNA Seq database file */
    @Argument(index = 0, metaVar = "rnaData.ser", usage = " name of the RNA Seq database file")
    private File rnaFile;

    @Override
    protected void setReporterDefaults() {
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        // Verify and load the RNA seq data.
        if (! this.rnaFile.canRead())
            throw new FileNotFoundException("RNA data input file " + this.rnaFile + " is not found or unreadable.");
        try {
            log.info("Loading RNA Seq database.");
            RnaData data = RnaData.load(this.rnaFile);
            log.info("Extracting feature data.");
            this.feats = data.getRows().stream().map(x -> x.getFeat()).sorted(new RnaFeatureData.BSort())
                    .collect(Collectors.toList());
            log.info("{} features found.", this.feats.size());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        writer.println("b-number\tgene\toperon\tregulon\tmodulons");
        log.info("Producing output.");
        // We will count the missing b-numbers in here.
        int missing = 0;
        // Loop through the features.
        for (RnaFeatureData feat : this.feats) {
            String bNum = feat.getBNumber();
            if (bNum.isEmpty())
                missing++;
            else {
                String modString = StringUtils.join(feat.getiModulons(), ',');
                writer.format("%s\t%s\t%s\tAR%d\t%s%n", bNum, feat.getGene(),
                        feat.getOperon(), feat.getAtomicRegulon(),
                        modString);
            }
        }
        log.info("{} features skipped due to missing b-numbers.", missing);
    }

}
