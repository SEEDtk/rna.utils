/**
 *
 */
package org.theseed.rna.erdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.regex.Matcher;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.Test;

/**
 * @author Bruce Parrello
 *
 */
class JsoupTest {

    @Test
    void test() throws IOException {
        // SAMSTAT produces crap HTML, so if we call the file ".html", Eclipse finds tons of errors.
        File samstatFile = new File("data", "test.samstat.htmlText");
        Document samStat = Jsoup.parse(samstatFile, StandardCharsets.UTF_8.toString());
        // First, we get the counts and the date from the description line.
        Element description = samStat.selectFirst("h1 + p");
        assertThat(description, not(nullValue()));
        String descriptionText = description.text();
        Matcher m = SampleUploadProcessor.SAMSTAT_STATS_LINE.matcher(descriptionText);
        assertThat("Description line pattern does not work.", m.matches());
        int readCount = Integer.valueOf(m.group(1));
        int baseCount = Integer.valueOf(m.group(2));
        LocalDate procDate = LocalDate.parse(m.group(3));
        assertThat(readCount, equalTo(278058));
        assertThat(baseCount, equalTo(20297046));
        assertThat(procDate, equalTo(LocalDate.of(2021, 2, 10)));
        // Now get the quality table.
        Element qualCell = samStat.selectFirst("tr:contains(MAPQ >= 30) > td:last-of-type");
        assertThat(qualCell, not(nullValue()));
        double qual = Double.valueOf(qualCell.text());
        assertThat(qual, closeTo(32.2, 0.001));
    }
}
