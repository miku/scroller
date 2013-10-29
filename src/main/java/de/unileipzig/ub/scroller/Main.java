package de.unileipzig.ub.scroller;

import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.search.SearchHit;

/**
 * Hello world!
 *
 * Copyright (C) 2012 Martin Czygan, martin.czygan@uni-leipzig.de
 * Leipzig University Library, Project finc
 * http://www.ub.uni-leipzig.de
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * @author   Martin Czygan
 * @license  http://opensource.org/licenses/gpl-3.0.html GNU General Public License
 * @link     http://finc.info
 *
 */
public class Main {

    private static Logger logger = Logger.getLogger(Main.class.getCanonicalName());

    /**
     * Parse a list of key=value or key=value,value,... pairs into a map.
     *
     * @param kvStrings
     * @return A map.
     * @throws ParseException
     */
    public static Map<String, String> getMapForKeys(final String[] kvStrings)
        throws ParseException {
            // accept key=value or key=value,value,value 
            final HashMap kvMap = new HashMap<String, String>();
            for (String kvString : kvStrings) {
                final String[] topLevelParts = kvString.split("=");
                if (topLevelParts.length != 2) {
                    throw new ParseException("Syntax for key value pairs "
                            + "is key=value or key=value1,value2,...");
                }
                final String key = topLevelParts[0];
                final String valueOrValues = topLevelParts[1];

                final String[] values = valueOrValues.split(",");

                if (values.length < 0) {
                    throw new ParseException("Key " + key + " has no value.");
                }

                if (values.length > 1) {
                throw new ParseException("Key " + key + " has too many values.");
            }

            if (values.length == 1) {
                kvMap.put(key, values[0]);
            } else {
                kvMap.put(key, new ArrayList<String>(Arrays.asList(values)));
            }
        }
        return kvMap;
    }
    
    public static Collection<HashMap> getFilterList(final String[] kvStrings) throws ParseException {
        
        final ArrayList<HashMap> filterList = new ArrayList<HashMap>();
        
        for (String kvString : kvStrings) {
            
            HashMap filterMap = new HashMap();
            
            final String[] topLevelParts = kvString.split("=");
            if (topLevelParts.length != 2) {
                throw new ParseException("Syntax for key value pairs "
                        + "is key=value or key=value1,value2,...");
            }
            final String key = topLevelParts[0];
            final String valueOrValues = topLevelParts[1];

            final String[] values = valueOrValues.split(",");

            if (values.length < 0) {
                throw new ParseException("Key " + key + " has no value.");
            }

            if (values.length > 1) {
                throw new ParseException("Key " + key + " has too many values.");
            }

            if (values.length == 1) {
                filterMap.put(key, values[0]);
            } else {
                filterMap.put(key, new ArrayList<String>(Arrays.asList(values)));
            }
            filterList.add(filterMap);
        }
        return filterList;
        
    }

    public static void main(String[] args) throws IOException {


        Options options = new Options();
        // add t option
        options.addOption("h", "help", false, "display this help");

        // elasticsearch options
        options.addOption("t", "host", true, "elasticsearch hostname (default: 0.0.0.0)");
        options.addOption("p", "port", true, "transport port (that's NOT the http port, default: 9300)");
        options.addOption("c", "cluster", true, "cluster name (default: elasticsearch_mdma)");

        options.addOption("i", "index", true, "index to use");

        options.addOption("f", "filter", true, "filter(s) - e.g. meta.kind=title");
        options.addOption("j", "junctor", true, "values: and, or (default: and)");
        options.addOption("n", "notice-every", true, "show speed after every N items");

        options.addOption("v", "verbose", false, "be verbose");
        // options.addOption("z", "end-of-message", true, "sentinel to print to stdout, once the regular input finished (default: EOM)");


        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException ex) {
            logger.error(ex);
            System.exit(1);
        }

        // process options
        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("scroller", options, true);
            System.exit(0);
        }
        
        String endOfMessage = "EOM";

        boolean verbose = false;
        if (cmd.hasOption("verbose")) {
            verbose = true;
        }

        if (!cmd.hasOption("i")) {
            System.err.println("error: no index specified");
            System.exit(1);
        }
        
        long noticeEvery = 10000;
        if (cmd.hasOption("n")) {
            noticeEvery = Long.parseLong(cmd.getOptionValue("n"));
        }

        // ES options
        String[] hosts = new String[]{"0.0.0.0"};
        int port = 9300;
        String clusterName = "elasticsearch_mdma";
        int bulkSize = 3000;

        if (cmd.hasOption("host")) {
            hosts = cmd.getOptionValues("host");
        }
        if (cmd.hasOption("port")) {
            port = Integer.parseInt(cmd.getOptionValue("port"));
        }
        if (cmd.hasOption("cluster")) {
            clusterName = cmd.getOptionValue("cluster");
        }

        // Index
        String indexName = cmd.getOptionValue("index");

        Map<String, String> filterMap = new HashMap<String, String>();
        if (cmd.hasOption("filter")) {
            try {
                filterMap = getMapForKeys(cmd.getOptionValues("filter"));
            } catch (ParseException pe) {
                System.err.println(pe);
                System.exit(1);
            }
        }
        
        Collection<HashMap> filterList = new ArrayList<HashMap>();
        if (cmd.hasOption("filter")) {
            try {
                filterList = getFilterList(cmd.getOptionValues("filter"));
            } catch (ParseException pe) {
                System.err.println(pe);
                System.exit(1);
            }
        }


        // ES Client
        final Settings settings = ImmutableSettings.settingsBuilder()
            .put("cluster.name", "elasticsearch_mdma")
            .put("client.transport.ping_timeout", "60s")
            .build();
        final TransportClient client = new TransportClient(settings);
        for (String host : hosts) {
            client.addTransportAddress(new InetSocketTransportAddress(host, port));
        }


        // build the query
        String junctor = "and";
        if (cmd.hasOption("j")) {
            junctor = cmd.getOptionValue("j");
        }

//        ArrayList<TermFilterBuilder> filters = new ArrayList<TermFilterBuilder>();
//        if (filterMap.size() > 0) {
//            for (Map.Entry<String, String> entry : filterMap.entrySet()) {
//                filters.add(new TermFilterBuilder(entry.getKey(), entry.getValue()));
//            }
//        }

        ArrayList<TermFilterBuilder> filters = new ArrayList<TermFilterBuilder>();
        if (filterList.size() > 0) {
            for (HashMap map : filterList) {
                for (Object obj : map.entrySet()) {
                    Map.Entry entry = (Map.Entry) obj;
                    filters.add(new TermFilterBuilder(entry.getKey().toString(), entry.getValue().toString()));
                }
            }
        }
        
        
        FilterBuilder fb = null;
        if (junctor.equals("and")) {
            AndFilterBuilder afb = new AndFilterBuilder();
            for (TermFilterBuilder tfb : filters) {
                afb.add(tfb);
            }
            fb = afb;
        }

        if (junctor.equals("or")) {
            OrFilterBuilder ofb = new OrFilterBuilder();
            for (TermFilterBuilder tfb : filters) {
                ofb.add(tfb);
            }
            fb = ofb;
        }

//        TermFilterBuilder tfb0 = new TermFilterBuilder("meta.kind", "title");
//        TermFilterBuilder tfb1 = new TermFilterBuilder("meta.timestamp", "201112081240");
//
//        AndFilterBuilder afb0 = new AndFilterBuilder(tfb0, tfb1);

        QueryBuilder qb0 = null;
        if (filterMap.isEmpty()) {
            qb0 = matchAllQuery();
        } else {
            qb0 = filteredQuery(matchAllQuery(), fb);
        }
        
        // sorting
        // FieldSortBuilder sortBuilder = new FieldSortBuilder("meta.timestamp");
        // sortBuilder.order(SortOrder.DESC);
        

        // FilteredQueryBuilder fqb0 = filteredQuery(matchAllQuery(), tfb0);

        final CountResponse countResponse = client.prepareCount(indexName).setQuery(qb0).execute().actionGet();
        final long total = countResponse.getCount();

        SearchResponse scrollResp = client
                .prepareSearch(indexName)
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .setQuery(qb0)
                // .addSort(sortBuilder) // sort has no effect on scroll type (see: https://github.com/CPAN-API/cpan-api/issues/172)
                .setSize(1000) //1000 hits per shard will be returned for each scroll
                .execute()
                .actionGet(); 

        //Scroll until no hits are returned
        
        System.err.println("[Scroller] query: " + qb0.toString());
        System.err.println("[Scroller] took: " + scrollResp.getTookInMillis() + "ms");
        System.err.println("[Scroller] docs found: " + total);

        long counter = 0;
        long start = System.currentTimeMillis();

        while (true) {
            scrollResp = client
                .prepareSearchScroll(scrollResp
                    .getScrollId())
                    .setScroll(
                        new TimeValue(600000))
                .execute()
                .actionGet();

            if (scrollResp.getHits().hits().length == 0) {
                break;
            }

            for (SearchHit hit : scrollResp.getHits()) {

                System.out.println(hit.sourceAsString());
                counter += 1;
                if (counter % noticeEvery == 0) {
                    final double elapsed = (System.currentTimeMillis() - start) / 1000;
                    final double speed = counter / elapsed;
                    final long eta = (long) ((elapsed / counter) * (total - counter) * 1000);
                    System.err.println(counter + "/" + total + " records recvd @ speed " + String.format("%1$,.1f", speed) + " r/s eta: " + DurationFormatUtils.formatDurationWords(eta, false, false));
                }
            }
        }
        System.out.close();
        // System.out.println(endOfMessage);
    }
}
