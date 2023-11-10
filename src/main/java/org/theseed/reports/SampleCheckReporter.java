/**
 *
 */
package org.theseed.reports;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.rna.RnaData;

/**
 * This is the base class for all of the RNA Seq expression database reports.  These reports
 * do not have a lot in common, but all require similar printing methods provided by this
 * method's subclass.
 *
 * @author Bruce Parrello
 *
 */
public abstract class SampleCheckReporter extends BaseReporterReporter {

    /**
     * This interface describes the data retrieval functions required of each controlling
     * command processor.
     */
    public interface IParms {

        /**
         * @return TRUE if only good samples should be included, else FALSE
         */
        boolean getPure();

        /**
         * @return the name of a file containing a tabular cluster report
         */
        File getClusterFile();

    }

    /**
     * This enum describes the different report types.
     */
    public static enum Type {
        BASELINE {
            @Override
            public SampleCheckReporter create(IParms processor) throws ParseFailureException, IOException {
                return new BaselineSampleCheckReporter(processor);
            }
        }, CORRELATION {
            @Override
            public SampleCheckReporter create(IParms processor) throws ParseFailureException, IOException {
                return new CorrelationSampleCheckReporter(processor);
            }
        }, WEIGHTED {
            @Override
            public SampleCheckReporter create(IParms processor) throws ParseFailureException, IOException {
                return new WeightedBaselineSampleCheckReporter(processor);
            }
        };

        public abstract SampleCheckReporter create(IParms processor) throws ParseFailureException, IOException;
    }

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SampleCheckReporter.class);

    /**
     * Generate this report.
     *
     * @param data	RNA seq expression database
     */
    public abstract void generate(RnaData data);

}
