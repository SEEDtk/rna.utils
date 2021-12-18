package org.theseed.rna.utils;

import java.util.Arrays;

import org.theseed.rna.erdb.ClusterLoadProcessor;
import org.theseed.rna.erdb.DbLoadProcessor;
import org.theseed.rna.erdb.MetaLoadProcessor;
import org.theseed.rna.erdb.SampleUploadProcessor;
import org.theseed.utils.BaseProcessor;

/**
 * Commands for utilities relating to RNA-Seq processing.
 *
 * fpkm			run jobs to convert FASTQ files to FPKM results
 * fpkmSummary	produce a summary file from FPKM results
 * fpkmAll		generate all of the standard RNA SEQ files
 * rnaCopy		copy RNA read files into PATRIC for processing by the FPKM commands
 * rnaSetup		update the sampleMeta.tbl file from the progress.txt and rna.production.tbl files
 * rnaProdFix	add production and density data to a sampleMeta.tbl file
 * rnaMaps		consolidate RNA maps from batch expression data runs
 * rnaCorr		determine the +/0/- correlation between genes in an RNA database
 * bGroups		read an RNA database and output the group IDs, organized by Blattner number
 * blacklist	remove correlation pairs relating to operons from a correlation file
 * sampleCheck	analyze variability in the expression data within a database
 * clusterCheck	perform comparative analysis of sample clusters
 * split		split a large database into clusters
 * sheets		create spreadsheets of the sample clusters output by the split sub-command
 * metaLoad		load genome metadata into an SQL database
 * dbLoad		load an RNA database into an SQL database
 * clusterLoad	load sample clustering data into an SQL database
 * download		download a set of RNA results from PATRIC and produce a metadata file for them
 */
public class App
{
    public static void main( String[] args )
    {
        // Get the control parameter.
        String command = args[0];
        String[] newArgs = Arrays.copyOfRange(args, 1, args.length);
        BaseProcessor processor;
        // Determine the command to process.
        switch (command) {
        case "fpkm" :
            processor = new RnaSeqProcessor();
            break;
        case "fpkmsummary" :
            processor = new FpkmSummaryProcessor();
            break;
        case "fpkmall" :
            processor = new FpkmAllProcessor();
            break;
        case "rnaCopy" :
            processor = new RnaCopyProcessor();
            break;
        case "rnaSetup" :
            processor = new SampleMetaProcessor();
            break;
        case "rnaProdFix" :
            processor = new SampleMetaFixProcessor();
            break;
        case "rnaMaps" :
            processor = new RnaMapProcessor();
            break;
        case "rnaCorr" :
            processor = new RnaCorrelationProcessor();
            break;
        case "bGroups" :
            processor = new BlattnerProcessor();
            break;
        case "blacklist" :
            processor = new BlacklistProcessor();
            break;
        case "sampleCheck" :
            processor = new SampleCheckProcessor();
            break;
        case "split" :
            processor = new ClusterSplitProcessor();
            break;
        case "sheets" :
            processor = new ClusterSheetProcessor();
            break;
        case "metaLoad" :
            processor = new MetaLoadProcessor();
            break;
        case "dbLoad" :
            processor = new DbLoadProcessor();
            break;
        case "clusterLoad" :
            processor = new ClusterLoadProcessor();
            break;
        case "download" :
            processor = new SampleDownloadProcessor();
            break;
        case "upload" :
            processor = new SampleUploadProcessor();
            break;
        default:
            throw new RuntimeException("Invalid command " + command);
        }
        // Process it.
        boolean ok = processor.parseCommand(newArgs);
        if (ok) {
            processor.run();
        }
    }
}
