/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.erdb.utils.BaseDbProcessor;
import org.theseed.erdb.utils.DbCollectors;
import org.theseed.genome.Feature;
import org.theseed.genome.Genome;
import org.theseed.genome.SubsystemRow;
import org.theseed.io.TabbedLineReader;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbLoader;
import org.theseed.java.erdb.DbQuery;
import org.theseed.utils.ParseFailureException;

/**
 * This command loads the metadata for a genome into an RNA Seq database.  If the genome is
 * already present in the database, it will be deleted.  This necessitates reloading the
 * samples, which is performed by the "DbLoadProcessor" command.
 *
 * The postional parameters should be the name of the GTO file for the source genome, the
 * name of the groups file, and the name of the subsystem map.
 *
 * The groups file is a tab-delimited file with headers.  The first column contains feature IDs.
 * The remaining columns contain group information.  Each column header contains the type of
 * group.  Within the columns, each feature's groups of the appropriate type are listed
 * with comma delimiters.  There should be no spaces around the commas.  The group ID in
 * this case will be the group name preceded by the genome ID (e.g. "511145.183:AR12").
 *
 * The subsystem map is also tab-delimited with headers.  The first column contains subsystem
 * IDs and the second the subsystem name that would be found in the GTO.  Subsystems, unlike
 * the other groups, can span genomes, so in this case the ID and name are unaltered in the
 * database.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 * --alias		regular-expression pattern for finding the secondary alias
 *
 * @author Bruce Parrello
 *
 */
public class MetaLoadProcessor extends BaseDbProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(MetaLoadProcessor.class);
    /** reference genome */
    private Genome refGenome;
    /** reference genome ID  */
    private String genomeId;
    /** map of subsystem names to IDs */
    private Map<String, String> subNameMap;
    /** B-number match pattern */
    private Pattern aliasPattern;
    /** set of features loaded into the genome */
    private Set<String> fidsLoaded;
    /** default alias pattern */
    static final String DEFAULT_ALIAS = "b\\d+";
    /** gene name match pattern */
    private static final Pattern GENE_NAME = Pattern.compile("[a-z]{3}(?:[A-Z])?");

    // COMMAND-LINE OPTIONS

    /** match pattern for the alternate alias */
    @Option(name = "--alias", metaVar = "gi\\|\\d+", usage = "match pattern for the alternate feature alias")
    private String aliasRegex;

    /** GTO file for the reference genome */
    @Argument(index = 0, metaVar = "refGenome.gto", usage = "GTO file for the reference genome", required = true)
    private File gtoFile;

    /** groups file */
    @Argument(index = 1, metaVar = "groups.tbl", usage = "file containing the groups for each feature", required = true)
    private File groupFile;

    /** subsystem map */
    @Argument(index = 2, metaVar = "subMap.tbl", usage = "subsystem ID/name mapping file", required = true)
    private File subMapFile;

    @Override
    protected void setDbDefaults() {
        this.aliasRegex = DEFAULT_ALIAS;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Verify the alias pattern.
        this.aliasPattern = Pattern.compile(this.aliasRegex);
        // Verify the groups file.
        if (! this.groupFile.canRead())
            throw new FileNotFoundException("Groups file " + this.groupFile + " is not found or unreadable.");
        // Load the subsystem map.
        if (! this.subMapFile.canRead())
            throw new FileNotFoundException("Subsystem mapping file " + this.subMapFile + " is not found or unreadable.");
        log.info("Loading subsystem mapping from {}.", this.subMapFile);
        // Note that the map key (subsystem name) is in column 2, and the value (subsystem ID) in column 1.
        this.subNameMap = TabbedLineReader.readMap(this.subMapFile, "2", "1");
        log.info("{} subsystems found.", this.subNameMap.size());
        // Load the genome.
        if (! this.gtoFile.canRead())
            throw new FileNotFoundException("Reference genome GTO file " + this.gtoFile + " is not found or unreadable.");
        log.info("Loading reference genome from {}.", this.gtoFile);
        this.refGenome = new Genome(this.gtoFile);
        log.info("Reference genome is {}.", this.refGenome);
        // Denote no features have been loaded.
        this.fidsLoaded = new HashSet<String>(this.refGenome.getFeatureCount());
        // Cache the reference genome ID.
        this.genomeId = this.refGenome.getId();
        return true;
    }

    @Override
    protected void runDbCommand(DbConnection db) throws Exception {
        // Check for the existence of the genome record.
        if (db.checkForRecord("Genome", this.genomeId)) {
            // Here we have to delete the old genome.
            log.info("Deleting old version of genome {}.", this.genomeId);
            db.deleteRecord("Genome", this.genomeId);
        }
        // Get a list of the feature IDs in a reasonable order.
        List<Feature> pegs = this.refGenome.getPegs().stream().sorted(new Feature.LocationComparator())
                .collect(Collectors.toList());
        // Now we can begin updating the database.
        try (var xact = db.new Transaction()) {
            log.info("Inserting genome {} into the Genome table.", this.genomeId);
            try (DbLoader loader = DbLoader.batch(db, "Genome")) {
                loader.set("genome_id", this.genomeId);
                loader.set("genome_name", this.refGenome.getName());
                loader.set("genome_len", this.refGenome.getLength());
                loader.set("peg_count", pegs.size());
                loader.insert();
            }
            // Now we need to add the features.  These are taken from the GTO.  We only do the pegs.
            try (DbLoader loader = DbLoader.batch(db, "Feature")) {
                // All the features will point to the genome.
                loader.set("genome_id", this.genomeId);
                // This counter will be used for the sequence number.
                int seqNo = 0;
                // Loop through the pegs.
                for (Feature feat : pegs) {
                    // Store the feature ID.
                    loader.set("fig_id", feat.getId());
                    // Store the location.
                    loader.set("location", feat.getLocation());
                    // Store the sequence number.
                    loader.set("seq_no", seqNo);
                    seqNo++;
                    // Store the function.
                    loader.set("assignment", feat.getPegFunction());
                    // Now process the aliases.
                    Collection<String> aliases = feat.getAliases();
                    this.storeAlias(loader, aliases, "alias", this.aliasPattern);
                    this.storeAlias(loader, aliases, "gene_name", GENE_NAME);
                    // Insert the feature.
                    loader.insert();
                    this.fidsLoaded.add(feat.getId());
                }
            }
            // What remains is to load the groups.
            this.loadSubsystems(db);
            this.loadGroupFile(db);
            // Commit our changes.
            xact.commit();
        }
    }

    /**
     * Load all the groups from the group file.
     *
     * @param db		target database
     *
     * @throws SQLException
     * @throws IOException
     */
    private void loadGroupFile(DbConnection db) throws SQLException, IOException {
        // This set tracks the groups that already exist.  All of the groups we are
        // loading now are genome-specific, so we use a pattern query.  We only keep
        // the group name, not the full ID, but it is unique, since group names are
        // unique within a genome.
        Set<String> groups;
        try (DbQuery query = new DbQuery(db, "FeatureGroup")) {
            query.select("FeatureGroup", "group_name");
            groups = query.stream().collect(DbCollectors.set("FeatureGroup.group_name"));
        }
        try (DbLoader connectLoader = DbLoader.batch(db, "FeatureToGroup");
                DbLoader groupLoader = DbLoader.single(db, "FeatureGroup");
                TabbedLineReader groupStream = new TabbedLineReader(this.groupFile)) {
            // Get the column headers.  These determine the group types.
            String[] headers = groupStream.getLabels();
            // Now loop through the features.
            for (TabbedLineReader.Line line : groupStream) {
                // Set the connection loader for this feature.
                connectLoader.set("fig_id", line.get(0));
                // Process each group type.
                for (int i = 1; i < headers.length; i++) {
                    groupLoader.set("group_type", headers[i]);
                    String[] groupNames = StringUtils.split(line.get(i), ',');
                    for (String groupName : groupNames) {
                        String groupId = this.genomeId + ":" + groupName;
                        // Insure the group exists.
                        if (! groups.contains(groupName)) {
                            log.info("Inserting group record for {}.", groupId);
                            groupLoader.set("group_id", groupId);
                            groupLoader.set("group_name", groupName);
                            groupLoader.insert();
                            groups.add(groupName);
                        }
                        // Connect the feature to it.
                        connectLoader.set("group_id", groupId);
                    }
                }
            }
        }
    }

    /**
     * Load all the subsystems present in the genome.
     *
     * @param db		target database
     *
     * @throws SQLException
     * @throws IOException
     */
    public void loadSubsystems(DbConnection db) throws SQLException, IOException {
        try (DbLoader groupLoader = DbLoader.single(db, "FeatureGroup");
                DbLoader connectLoader = DbLoader.batch(db, "FeatureToGroup")) {
            // All the groups in this pass will be type subsystem.
            groupLoader.set("group_type", "subsystem");
            for (SubsystemRow subRow : this.refGenome.getSubsystems()) {
                // Get all the features in this subsystem.  Note we have to filter out features that
                // don't really exist.
                Set<String> fids = subRow.getRoles().stream().flatMap(x -> x.getFeatures().stream())
                        .map(x -> x.getId()).filter(x -> this.fidsLoaded.contains(x))
                        .collect(Collectors.toSet());
                if (! fids.isEmpty()) {
                    // Here there are features to connect.  Get the subsystem ID.
                    String subName = subRow.getName();
                    String subId = this.subNameMap.get(subName);
                    if (subId == null)
                        throw new IOException("Subsystem name \"" + subName + "\" is not in " + this.subMapFile + ".");
                    // Insure the group exists.
                    if (! db.checkForRecord("FeatureGroup", subId)) {
                        log.info("Inserting group record for {}: {}.", subId, subName);
                        groupLoader.set("group_id", subId);
                        groupLoader.set("group_name", subName);
                        groupLoader.insert();
                    }
                    // Make the connections.
                    connectLoader.set("group_id", subId);
                    for (String fid : fids) {
                        connectLoader.set("fig_id", fid);
                        connectLoader.insert();
                    }
                    log.info("{} features connected to {}.", fids.size(), subId);
                }
            }
        }
    }

    /**
     * Store an alias field in the specified loader.
     *
     * @param loader	loader for the feature table
     * @param aliases	list of aliases for the feature
     * @param field		name of the field to store
     * @param pattern	match pattern for the alias
     *
     * @throws SQLException
     */
    private void storeAlias(DbLoader loader, Collection<String> aliases, String field, Pattern pattern) throws SQLException {
        Optional<String> alias = aliases.stream().filter(x -> pattern.matcher(x).matches()).findFirst();
        if (alias.isPresent())
            loader.set(field, alias.get());
        else
            loader.setNull(field);
    }

}
