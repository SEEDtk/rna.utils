/**
 *
 */
package org.theseed.rna.jobs;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.theseed.cli.RnaSource;
import org.theseed.io.TabbedLineReader;

/**
 * This is an RNA sequence group consisting of samples in the NCBI SRA.  Each sample is represented by
 * a run accession ID.  The IDs are in the first column of the specified file.
 *
 * @author Bruce Parrello
 *
 */
public class SraRnaSeqGroup extends RnaSeqGroup {

    /**
     * No special parameters are required, but we need a constructor that takes an
     * IParms input just in case.
     *
     * @param processor  controlling command processor
     */
    public SraRnaSeqGroup(IParms processor) {
    }

    @Override
    public Map<String, RnaJob> getJobs(String inFile) throws IOException {
        // Read in all the SRA numbers.
        File actualFile = new File(inFile);
        Set<String> srrList = TabbedLineReader.readSet(actualFile, "1");
        log.info("{} SRR IDs read from {}.", srrList.size(), actualFile);
        // Create the output hash.
        Map<String, RnaJob> retVal = new HashMap<String, RnaJob>(srrList.size() * 4 / 3);
        // Now create a job for each one.
        for (String srrId : srrList) {
            RnaJob job = new RnaJob(srrId, this.getOutDir(), this.getGenomeId());
            RnaSource source = new RnaSource.SRA(srrId);
            job.setSource(source);
            retVal.put(srrId, job);
        }
        return retVal;
    }

}
