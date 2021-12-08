/**
 *
 */
package org.theseed.genome;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.theseed.test.Matchers.*;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Test;

/**
 * @author Bruce Parrello
 *
 */
class TestClusterData {

    @Test
    void test() throws IOException {
        File testFile = new File("data", "test.clusters.tbl");
        var testMap = ClusterFeatureData.readMap(testFile);
        assertThat(testMap.size(), equalTo(6));
        assertThat(testMap.containsKey("fig|511145.183.peg.1010"), isFalse());
        ClusterFeatureData test1 = testMap.get("fig|511145.183.peg.11");
        assertThat(test1.getClusterId(), equalTo("CL1"));
        assertThat(test1.getFid(), equalTo("fig|511145.183.peg.11"));
        assertThat(test1.getFunction(), equalTo("Hypothetical protein YaaW, modifier of biofilm"));
        assertThat(test1.getGene(), equalTo("yaaW"));
        assertThat(test1.getSubsystems().length, equalTo(0));
        test1 = testMap.get("fig|511145.183.peg.35");
        assertThat(test1.getClusterId(), equalTo("CL1"));
        assertThat(test1.getFid(), equalTo("fig|511145.183.peg.35"));
        assertThat(test1.getFunction(), equalTo("Carnitine operon protein CaiE"));
        assertThat(test1.getGene(), equalTo("caiE"));
        assertThat(test1.getSubsystems(), arrayContaining("CarnMetaTran", "CarnOper"));
        test1 = testMap.get("fig|511145.183.peg.42");
        assertThat(test1.getClusterId(), equalTo("CL1"));
        assertThat(test1.getFid(), equalTo("fig|511145.183.peg.42"));
        assertThat(test1.getFunction(), equalTo("Electron transfer flavoprotein, alpha subunit ecFixB"));
        assertThat(test1.getGene(), equalTo("fixB"));
        assertThat(test1.getSubsystems().length, equalTo(0));
        test1 = testMap.get("fig|511145.183.peg.704");
        assertThat(test1.getClusterId(), equalTo("CL2"));
        assertThat(test1.getFid(), equalTo("fig|511145.183.peg.704"));
        assertThat(test1.getFunction(), equalTo("Ornithine decarboxylase, fold type I (EC 4.1.1.17) ## inducible"));
        assertThat(test1.getGene(), equalTo("speF"));
        assertThat(test1.getSubsystems(), arrayContaining("ArgiDecaAgmaClus", "CadbOperItSRolePh", "PolyMeta",
                "PutrViaOrni"));
        test1 = testMap.get("fig|511145.183.peg.718");
        assertThat(test1.getClusterId(), equalTo("CL3"));
        assertThat(test1.getFid(), equalTo("fig|511145.183.peg.718"));
        assertThat(test1.getFunction(), equalTo("H repeat-associated protein, YhhI family"));
        assertThat(test1.getGene(), equalTo(""));
        assertThat(test1.getSubsystems().length, equalTo(0));
        test1 = testMap.get("fig|511145.183.peg.2396");
        assertThat(test1.getClusterId(), equalTo("CL4"));
        assertThat(test1.getFid(), equalTo("fig|511145.183.peg.2396"));
        assertThat(test1.getFunction(), equalTo("Flagellar regulator flk"));
        assertThat(test1.getGene(), equalTo("flk"));
        assertThat(test1.getSubsystems(), arrayContaining("Flag"));
    }

}
