/*
Copyright 2020 Curtin University

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Author: Richard Hosking
*/

{% macro create_aggregate(doi_ref, aggregation_field, group_by_time_field, relate_to_institutions, relate_to_countries, relate_to_groups, relate_to_members, relate_to_journals, relate_to_funders, relate_to_publishers) %}

-- Helper Function: Processing Output Types
/*
Output Schema:
per_25th	FLOAT	NULLABLE
median	    FLOAT	NULLABLE
per_90th	FLOAT	NULLABLE
per_95th	FLOAT	NULLABLE
*/
CREATE TEMP FUNCTION compute_percentiles(counts ARRAY<INT64>) AS (
  (SELECT as STRUCT
    ROUND(PERCENTILE_CONT(count, 0.25) OVER(), 2) as per_25th,
    ROUND(PERCENTILE_CONT(count, 0.50) OVER(), 2) as median,
    ROUND(PERCENTILE_CONT(count, 0.90) OVER(), 2) as per_90th,
    ROUND(PERCENTILE_CONT(count, 0.95) OVER(), 2) as per_95th
  FROM UNNEST(counts) as count LIMIT 1)
);

-- Helper Function: Counting Access Types
/*
Output Schema:
num_oa_outputs	                        INTEGER	NULLABLE
num_in_doaj	                            INTEGER	NULLABLE
num_green_outputs	                    INTEGER	NULLABLE
num_gold_outputs	                    INTEGER	NULLABLE
num_gold_just_doaj_outputs	            INTEGER	NULLABLE
num_hybrid_outputs	                    INTEGER	NULLABLE
num_bronze_outputs	                    INTEGER	NULLABLE
num_green_only_outputs	                INTEGER	NULLABLE
num_green_only_ignoring_bronze_outputs	INTEGER	NULLABLE
num_has_license	                        INTEGER	NULLABLE
num_is_cclicensed	                    INTEGER	NULLABLE
*/
CREATE TEMP FUNCTION count_access_types(unpaywall ANY TYPE) as (
    (SELECT as STRUCT
      COUNTIF(is_oa) as num_oa_outputs,
      COUNTIF(is_in_doaj) as num_in_doaj,
      COUNTIF(green) as num_green_outputs,
      COUNTIF(gold) as num_gold_outputs,
      COUNTIF(gold_just_doaj) as num_gold_just_doaj_outputs,
      COUNTIF(hybrid) as num_hybrid_outputs,
      COUNTIF(bronze) as num_bronze_outputs,
      COUNTIF(green_only) as num_green_only_outputs,
      COUNTIF(green_only_ignoring_bronze) as num_green_only_ignoring_bronze_outputs,
      COUNTIF(has_license) as num_has_license,
      COUNTIF(is_cclicensed) as num_is_cclicensed
    FROM UNNEST(unpaywall))
);

-- Helper Function: Count distribution of access types and citations for a single output type
/*
Output Schema:
output_type	                STRING	NULLABLE
total_outputs	            INTEGER	NULLABLE
num_oa_outputs	            INTEGER	NULLABLE
num_green_outputs	        INTEGER	NULLABLE
num_gold_outputs	        INTEGER	NULLABLE
num_gold_just_doaj_outputs	INTEGER	NULLABLE
num_hybrid_outputs	        INTEGER	NULLABLE
num_bronze_outputs	        INTEGER	NULLABLE
num_green_only_outputs	    INTEGER	NULLABLE
citations	                RECORD	NULLABLE
citations.total_mag_citations	            INTEGER	NULLABLE
citations.total_open_citations_citations	INTEGER	NULLABLE
citations.total_crossref_citations	        INTEGER	NULLABLE
*/
CREATE TEMP FUNCTION count_single_output_type(
        output_type STRING,
        items ARRAY<STRUCT<type STRING, citations STRUCT<mag INT64, crossref INT64, open_citations INT64>,
        oa BOOL, green BOOL, gold BOOL, gold_just_doaj BOOL, hybrid BOOL, bronze BOOL, green_only BOOL>>, measured_type STRING)
  AS (
  (SELECT as STRUCT
    output_type,
    COUNTIF(item.type = measured_type ) as total_outputs,
    COUNTIF(item.type = measured_type AND item.oa ) as num_oa_outputs,
    COUNTIF(item.type = measured_type AND item.green ) as num_green_outputs,
    COUNTIF(item.type = measured_type AND item.gold ) as num_gold_outputs,
    COUNTIF(item.type = measured_type AND item.gold_just_doaj ) as num_gold_just_doaj_outputs,
    COUNTIF(item.type = measured_type AND item.hybrid ) as num_hybrid_outputs,
    COUNTIF(item.type = measured_type AND item.bronze ) as num_bronze_outputs,
    COUNTIF(item.type = measured_type AND item.green_only ) as num_green_only_outputs,
    STRUCT(
      SUM(citations.mag) as total_mag_citations,
      SUM(citations.open_citations) as total_open_citations_citations,
      SUM(citations.crossref) as total_crossref_citations
    ) as citations
  FROM UNNEST(items) as item)
);

-- Helper Function: Count distribution of access types, bucketing a number of individual output types into a new bucket
/*
Output Schema:
output_type	                STRING	NULLABLE
total_outputs	            INTEGER	NULLABLE
num_oa_outputs	            INTEGER	NULLABLE
num_green_outputs	        INTEGER	NULLABLE
num_gold_outputs	        INTEGER	NULLABLE
num_gold_just_doaj_outputs	INTEGER	NULLABLE
num_hybrid_outputs	        INTEGER	NULLABLE
num_bronze_outputs	        INTEGER	NULLABLE
num_green_only_outputs	    INTEGER	NULLABLE
citations	                RECORD	NULLABLE
citations.total_mag_citations	            INTEGER	NULLABLE
citations.total_open_citations_citations	INTEGER	NULLABLE
citations.total_crossref_citations	        INTEGER	NULLABLE
*/
CREATE TEMP FUNCTION count_array_output_type(
        output_type STRING, items ARRAY<STRUCT<type STRING, citations STRUCT<mag INT64, crossref INT64, open_citations INT64>,
        oa BOOL, green BOOL, gold BOOL, gold_just_doaj BOOL, hybrid BOOL, bronze BOOL, green_only BOOL>>, measured_type ARRAY<STRING>)
  AS (
  (SELECT as STRUCT
    output_type,
    COUNTIF(item.type in UNNEST(measured_type) ) as total_outputs,
    COUNTIF(item.type in UNNEST(measured_type) AND item.oa ) as num_oa_outputs,
    COUNTIF(item.type in UNNEST(measured_type) AND item.green ) as num_green_outputs,
    COUNTIF(item.type in UNNEST(measured_type) AND item.gold ) as num_gold_outputs,
    COUNTIF(item.type in UNNEST(measured_type) AND item.gold_just_doaj ) as num_gold_just_doaj_outputs,
    COUNTIF(item.type in UNNEST(measured_type) AND item.hybrid ) as num_hybrid_outputs,
    COUNTIF(item.type in UNNEST(measured_type) AND item.bronze ) as num_bronze_outputs,
    COUNTIF(item.type in UNNEST(measured_type) AND item.green_only ) as num_green_only_outputs,
    STRUCT(
      SUM(citations.mag) as total_mag_citations,
      SUM(citations.open_citations) as total_open_citations_citations,
      SUM(citations.crossref) as total_crossref_citations
    ) as citations

  FROM UNNEST(items) as item)
);

-- Helper Function: Count distribution of access types, where outputtype does not match any from a list
/*
Output Schema:
output_type	                STRING	NULLABLE
total_outputs	            INTEGER	NULLABLE
num_oa_outputs	            INTEGER	NULLABLE
num_green_outputs	        INTEGER	NULLABLE
num_gold_outputs	        INTEGER	NULLABLE
num_gold_just_doaj_outputs	INTEGER	NULLABLE
num_hybrid_outputs	        INTEGER	NULLABLE
num_bronze_outputs	        INTEGER	NULLABLE
num_green_only_outputs	    INTEGER	NULLABLE
citations	                RECORD	NULLABLE
citations.total_mag_citations	            INTEGER	NULLABLE
citations.total_open_citations_citations	INTEGER	NULLABLE
citations.total_crossref_citations	        INTEGER	NULLABLE
*/
CREATE TEMP FUNCTION count_not_in_array_output_type(
        output_type STRING, items ARRAY<STRUCT<type STRING, citations STRUCT<mag INT64, crossref INT64, open_citations INT64>,
        oa BOOL, green BOOL, gold BOOL, gold_just_doaj BOOL, hybrid BOOL, bronze BOOL, green_only BOOL>>, measured_type ARRAY<STRING>)
  AS (
  (SELECT as STRUCT
    output_type,
    COUNTIF(item.type not in UNNEST(measured_type) ) as total_outputs,
    COUNTIF(item.type not in UNNEST(measured_type) AND item.oa ) as num_oa_outputs,
    COUNTIF(item.type not in UNNEST(measured_type) AND item.green ) as num_green_outputs,
    COUNTIF(item.type not in UNNEST(measured_type) AND item.gold ) as num_gold_outputs,
    COUNTIF(item.type not in UNNEST(measured_type) AND item.gold_just_doaj ) as num_gold_just_doaj_outputs,
    COUNTIF(item.type not in UNNEST(measured_type) AND item.hybrid ) as num_hybrid_outputs,
    COUNTIF(item.type not in UNNEST(measured_type) AND item.bronze ) as num_bronze_outputs,
    COUNTIF(item.type not in UNNEST(measured_type) AND item.green_only ) as num_green_only_outputs,
    STRUCT(
      SUM(citations.mag) as total_mag_citations,
      SUM(citations.open_citations) as total_open_citations_citations,
      SUM(citations.crossref) as total_crossref_citations
    ) as citations
  FROM UNNEST(items) as item)
);

-- Helper Function: Count Output Types
/*
Output Schema:
output_types	RECORD	REPEATED

* Each record has the same schema, and is captured in the count_* methods
*/
CREATE TEMP FUNCTION count_output_types(
        items ARRAY<STRUCT<type STRING, citations STRUCT<mag INT64, crossref INT64, open_citations INT64>,
        oa BOOL, green BOOL, gold BOOL, gold_just_doaj BOOL, hybrid BOOL, bronze BOOL, green_only BOOL>>)
    AS (
    [
      count_single_output_type("journal_articles", items, 'journal-article'),
      count_array_output_type("book_sections", items, ['book-section', 'book-part', 'book-chapter']),
      count_array_output_type("authored_books", items, ['book', 'monograph']),
      count_single_output_type("edited_volumes", items, 'edited-book'),
      count_single_output_type("reports", items, 'report'),
      count_single_output_type("datasets", items, 'dataset'),
      count_single_output_type("proceedings_article", items, 'proceedings-article'),
      count_not_in_array_output_type("other_outputs", items, ['journal-article', 'book-section', 'book-part', 'book-chapter','book', 'monograph','edited-book', 'report', 'dataset', 'proceedings-article'])
    ]
);

-- Helper Function: Compute Citation Analysis
/*
Output Schema:
label	                    STRING	NULLABLE
access_type	                STRING	NULLABLE
status	                    BOOLEAN	NULLABLE
total_outputs	            INTEGER	NULLABLE
outputs_with_citations	    INTEGER	NULLABLE
outputs_without_citations	INTEGER	NULLABLE
citations	                RECORD	NULLABLE
citations.mag	                    RECORD	NULLABLE
citations.mag.total_citations	    INTEGER	NULLABLE
citations.mag.percentiles	        RECORD	NULLABLE
citations.open_citations	                RECORD	NULLABLE
citations.open_citations.total_citations	INTEGER	NULLABLE
citations.open_citations.percentiles	    RECORD	NULLABLE
citations.crossref                  RECORD	NULLABLE
citations.crossref.total_citations	INTEGER	NULLABLE
citations.crossref.percentiles	    RECORD	NULLABLE

*percetiles schema captured above
*/
CREATE TEMP FUNCTION compute_conditional_citations(
           items ARRAY<STRUCT<citations STRUCT<mag INT64, crossref INT64, open_citations INT64>, is_x BOOL>>,
           access_type STRING, positive_label STRING, negative_label STRING)
  AS (
  ARRAY((
    SELECT as STRUCT
      CASE
        WHEN is_x THEN positive_label
        WHEN NOT is_x THEN negative_label
        END
      as label,
      access_type,
      is_x as status,
      COUNT(*) as total_outputs,
      COUNTIF(citations.mag > 0 OR citations.crossref > 0 OR citations.open_citations > 0) as outputs_with_citations,
      COUNTIF( (citations.mag IS NULL OR citations.mag = 0) AND (citations.crossref IS NULL OR citations.crossref = 0) AND (citations.open_citations IS NULL OR citations.open_citations = 0) ) as outputs_without_citations,
      STRUCT(
        STRUCT(
          SUM(citations.mag) as total_citations,
          compute_percentiles(ARRAY_AGG(citations.mag)) as percentiles
        ) as mag,
        STRUCT(
          SUM(citations.open_citations) as total_citations,
          compute_percentiles(ARRAY_AGG(citations.open_citations)) as percentiles
        ) as open_citations, 
        STRUCT(
          SUM(citations.crossref) as total_citations,
          compute_percentiles(ARRAY_AGG(citations.crossref)) as percentiles
        ) as crossref
      ) as citations,

    FROM UNNEST(items)
    GROUP BY is_x
  ))
);

-- Helper Function: Compute a citation analysis across the array of passed in objects
/*
Output Schema:
mag	                        RECORD	NULLABLE
mag.total_citations	            INTEGER	NULLABLE
mag.citations_per_output	    FLOAT	NULLABLE
mag.outputs_with_citations	    INTEGER	NULLABLE
mag.outputs_without_citations	INTEGER	NULLABLE
mag.citations_per_cited_output	FLOAT	NULLABLE
crossref	                            RECORD	NULLABLE
crossref.total_citations	            INTEGER	NULLABLE
crossref.citations_per_output	        FLOAT	NULLABLE
crossref.outputs_with_citations	        INTEGER	NULLABLE
crossref.outputs_without_citations	    INTEGER	NULLABLE
crossref.citations_per_cited_output	    FLOAT	NULLABLE
open_citations	                            RECORD	NULLABLE
open_citations.total_citations	            INTEGER	NULLABLE
open_citations.citations_per_output	        FLOAT	NULLABLE
open_citations.output_with_citations	    INTEGER	NULLABLE
open_citations.outputs_without_citations	INTEGER	NULLABLE
open_citations.citations_per_cited_output	FLOAT	NULLABLE
*/
CREATE TEMP FUNCTION compute_citations(items ARRAY<STRUCT<doi STRING, citations STRUCT<mag INT64, crossref INT64, open_citations INT64>>>) as (
  (SELECT AS STRUCT
    -- Citation counts
    STRUCT(
      SUM(citations.mag) as total_citations,
      ROUND(SAFE_DIVIDE( SUM(citations.mag) , COUNT(doi)), 2) as citations_per_output,
      COUNTIF(citations.mag > 0) as outputs_with_citations,
      COUNTIF(citations.mag is null) as outputs_without_citations,
      ROUND(SAFE_DIVIDE(SUM(citations.mag), COUNTIF(citations.mag > 0)), 2) as citations_per_cited_output
    ) as mag,
    STRUCT(
      SUM(citations.crossref) as total_citations,
      ROUND(SAFE_DIVIDE( SUM(citations.crossref) , COUNT(doi)), 2) as citations_per_output,
      COUNTIF(citations.crossref > 0) as outputs_with_citations,
      COUNTIF(citations.crossref is null) as outputs_without_citations,
      ROUND(SAFE_DIVIDE(SUM(citations.crossref), COUNTIF(citations.crossref > 0)), 2) as citations_per_cited_output
    ) as crossref,
    STRUCT(
      SUM(citations.open_citations) as total_citations,
      ROUND(SAFE_DIVIDE( SUM(citations.open_citations) , COUNT(doi)), 2) as citations_per_output,
      COUNTIF(citations.open_citations > 0) as output_with_citations,
      COUNTIF(citations.open_citations is null) as outputs_without_citations,
      ROUND(SAFE_DIVIDE(SUM(citations.open_citations), COUNTIF(citations.open_citations > 0)), 2) as citations_per_cited_output
    ) as open_citations
  FROM UNNEST(items))
);

-- Helper Function: Compute the count of various Open Access Types from a passed in array of Objects
/*
Output Schema:
oa	                        RECORD	NULLABLE
oa.total_outputs	        INTEGER	NULLABLE
oa.percent	                FLOAT	NULLABLE
green	                    RECORD	NULLABLE
green.total_outputs	        INTEGER	NULLABLE
green.percent	            FLOAT	NULLABLE
gold	                    RECORD	NULLABLE
gold.total_outputs	        INTEGER	NULLABLE
gold.percent	            FLOAT	NULLABLE
gold_doaj	                RECORD	NULLABLE
gold_doaj.total_outputs	    INTEGER	NULLABLE
gold_doaj.percent	        FLOAT	NULLABLE
hybrid	                    RECORD	NULLABLE
hybrid.total_outputs	    INTEGER	NULLABLE
hybrid.percent	            FLOAT	NULLABLE
bronze	                    RECORD	NULLABLE
bronze.total_outputs	    INTEGER	NULLABLE
bronze.percent	            FLOAT	NULLABLE
green_only	                RECORD	NULLABLE
green_only.total_outputs	INTEGER	NULLABLE
green_only.percent	        FLOAT	NULLABLE
publisher	                RECORD	NULLABLE
publisher.total_outputs	    INTEGER	NULLABLE
publisher.percent	        FLOAT	NULLABLE
breakdown	                RECORD	REPEATED

* breakdown object array captured in compute_conditional_citations schema above
*/
CREATE TEMP FUNCTION compute_access_types(
           items ARRAY<STRUCT<doi STRING, citations STRUCT<mag INT64, crossref INT64, open_citations INT64>,
           is_oa BOOL, green BOOL, gold BOOL, gold_just_doaj BOOL, hybrid BOOL, bronze BOOL, green_only BOOL>>)
  AS (
  (SELECT AS STRUCT
    STRUCT(
      COUNTIF(is_oa = True) as total_outputs,
      ROUND(SAFE_DIVIDE( (COUNTIF(is_oa is True)) * 100 , COUNT(doi)), 2) as percent
    ) as oa,

    STRUCT(
      COUNTIF(green = True) as total_outputs,
      ROUND(SAFE_DIVIDE( (COUNTIF(green is True)) * 100 , COUNT(doi)), 2) as percent
    ) as green,

    STRUCT(
      COUNTIF(gold = True) as total_outputs,
      ROUND(SAFE_DIVIDE( (COUNTIF(gold is True)) * 100 , COUNT(doi)), 2) as percent
    ) as gold,

    STRUCT(
      COUNTIF(gold_just_doaj = True) as total_outputs,
      ROUND(SAFE_DIVIDE( (COUNTIF(gold_just_doaj is True)) * 100 , COUNT(doi)), 2) as percent
    ) as gold_doaj,

    STRUCT(
      COUNTIF(hybrid) as total_outputs,
      ROUND(SAFE_DIVIDE( (COUNTIF(hybrid is True)) * 100 , COUNT(doi)), 2) as percent
    ) as hybrid,

    STRUCT(
      COUNTIF(bronze) as total_outputs,
      ROUND(SAFE_DIVIDE( (COUNTIF(bronze is True)) * 100 , COUNT(doi)), 2) as percent
    ) as bronze,

    STRUCT(
      COUNTIF(green_only) as total_outputs,
      ROUND(SAFE_DIVIDE( (COUNTIF(green_only is True)) * 100 , COUNT(doi)), 2) as percent
    ) as green_only,

    STRUCT(
      COUNTIF(gold OR gold_just_doaj OR hybrid OR bronze) as total_outputs,
      ROUND(SAFE_DIVIDE( (COUNTIF(gold OR gold_just_doaj OR hybrid OR bronze)) * 100 , COUNT(doi)), 2) as percent
    ) as publisher,

    ARRAY_CONCAT(
      compute_conditional_citations(ARRAY_AGG(STRUCT(citations, IFNULL(is_oa, false) as is_x)), "oa", "Open Access", "Not Open Access"),
      compute_conditional_citations(ARRAY_AGG(STRUCT(citations, IFNULL(green, false) as is_x)), "green", "Green", "Not Green"),
      compute_conditional_citations(ARRAY_AGG(STRUCT(citations, IFNULL(gold, false) as is_x)), "gold", "Gold", "Not Gold"),
      compute_conditional_citations(ARRAY_AGG(STRUCT(citations, IFNULL(gold_just_doaj, false) as is_x)), "gold_just_doaj", "Gold just DOAJ", "Not Gold just DOAJ"),
      compute_conditional_citations(ARRAY_AGG(STRUCT(citations, IFNULL(hybrid, false) as is_x)), "hybrid", "Hybrid", "Not Hybrid"),
      compute_conditional_citations(ARRAY_AGG(STRUCT(citations, IFNULL(bronze, false) as is_x)), "bronze", "Bronze", "Not Bronze"),
      compute_conditional_citations(ARRAY_AGG(STRUCT(citations, IFNULL(green_only, false) as is_x)), "green_only", "Green Only", "Not Green Only")
    ) as breakdown

  FROM UNNEST(items))
);

-- Helper Function:  Compute summary statistic, grouped by a field of study for an array of passed in items
/*
Output Schema:
field	                        STRING	NULLABLE
total_outputs	                INTEGER	NULLABLE
sum_of_scores	                FLOAT	NULLABLE
citations	                    RECORD	NULLABLE
mag	                            RECORD	NULLABLE
total_citations	                INTEGER	NULLABLE
percentiles	                    RECORD	NULLABLE
open_citations	                RECORD	NULLABLE
total_citations	                INTEGER	NULLABLE
percentiles	                    RECORD	NULLABLE
crossref	                    RECORD	NULLABLE
total_citations	                INTEGER	NULLABLE
percentiles	                    RECORD	NULLABLE
num_oa_outputs	                INTEGER	NULLABLE
num_green_outputs	            INTEGER	NULLABLE
num_gold_outputs	            INTEGER	NULLABLE
num_gold_just_doaj_outputs	    INTEGER	NULLABLE
num_hybrid_outputs	            INTEGER	NULLABLE
num_bronze_outputs	            INTEGER	NULLABLE
num_green_only_outputs	        INTEGER	NULLABLE
funding	                                        RECORD	NULLABLE
total_funded_outputs	                            INTEGER	NULLABLE
num_international_outputs	                        INTEGER	NULLABLE
num_domestic_outputs	                            INTEGER	NULLABLE
num_international_and_domestic_outputs	            INTEGER	NULLABLE
num_government_outputs	                            INTEGER	NULLABLE
num_private_outputs	                                INTEGER	NULLABLE
num_government_and_private_outputs	                INTEGER	NULLABLE
num_international_collaboration_outputs	            INTEGER	NULLABLE
international_collaboration_with_funding_outputs	INTEGER	NULLABLE
*/
CREATE TEMP FUNCTION compute_disciplines(
            fields ARRAY<STRUCT<DisplayName STRING, Score FLOAT64, citations STRUCT<mag INT64, crossref INT64, open_citations INT64>,
            is_oa BOOL, green BOOL, gold BOOL, gold_just_doaj BOOL, hybrid BOOL, bronze BOOL, green_only BOOL,
            funding BOOL, international_funding BOOL, domestic_funding BOOL, government_funding BOOL, private_funding BOOL, international_colab BOOL>>)
  AS (
  ARRAY(
    (SELECT AS STRUCT
        DisplayName as field,
        COUNT(DisplayName) as total_outputs,
        SUM(Score) as sum_of_scores,
        STRUCT(
          STRUCT(
            SUM(citations.mag) as total_citations,
            compute_percentiles(ARRAY_AGG(citations.mag)) as percentiles
          ) as mag,
          STRUCT(
            SUM(citations.open_citations) as total_citations,
            compute_percentiles(ARRAY_AGG(citations.open_citations)) as percentiles
          ) as open_citations, 
          STRUCT(
            SUM(citations.crossref) as total_citations,
            compute_percentiles(ARRAY_AGG(citations.crossref)) as percentiles
          ) as crossref
        ) as citations,
        COUNTIF(is_oa) as num_oa_outputs, 
        COUNTIF(green) as num_green_outputs, 
        COUNTIF(gold) as num_gold_outputs, 
        COUNTIF(gold_just_doaj) as num_gold_just_doaj_outputs, 
        COUNTIF(hybrid) as num_hybrid_outputs, 
        COUNTIF(bronze) as num_bronze_outputs, 
        COUNTIF(green_only) as num_green_only_outputs,
        STRUCT(
          COUNTIF(funding) as total_funded_outputs,
          COUNTIF(international_funding) as num_international_outputs,
          COUNTIF(domestic_funding) as num_domestic_outputs,
          COUNTIF(international_funding AND domestic_funding) as num_international_and_domestic_outputs,
          COUNTIF(government_funding) as num_government_outputs,
          COUNTIF(private_funding) as num_private_outputs,
          COUNTIF(government_funding AND private_funding) as num_government_and_private_outputs
        ) as funding,
        COUNTIF(international_colab) as num_international_collaboration_outputs,
        COUNTIF(international_colab AND international_funding) as international_collaboration_with_funding_outputs
    FROM UNNEST(fields)
    GROUP BY DisplayName)
  )
);

-- Helper Function: Function to help with the process_relations function below. Simlifed summary, grouped by field of study
/*
Output Schema:
field	        STRING	NULLABLE
total_outputs	INTEGER	NULLABLE
num_oa_outputs	INTEGER	NULLABLE
sum_of_scores	FLOAT	NULLABLE
*/
CREATE TEMP FUNCTION group_disciplines(fields ARRAY<STRUCT<DisplayName STRING, Score FLOAT64, is_oa BOOL>>) as (
  ARRAY(
    (SELECT AS STRUCT
      DisplayName as field,
      COUNT(DisplayName) as total_outputs,
      COUNTIF(is_oa) as num_oa_outputs,
      ROUND(SUM(Score), 4) as sum_of_scores,
    FROM UNNEST(fields)
    GROUP BY DisplayName)
  )
);

-- Helper Function: Compute a summary of OA types, citations and fields of study. Use for compute collaborations and various other relationship to the primary entity
/*
Output Schema:
id	                                    STRING	NULLABLE
total_outputs	                        INTEGER	NULLABLE
percentage_of_all_outputs	            FLOAT	NULLABLE
percentage_of_all_oa	                FLOAT	NULLABLE
name	                                STRING	NULLABLE
country	                                STRING	NULLABLE
country_code	                        STRING	NULLABLE
region	                                STRING	NULLABLE
subregion	                            STRING	NULLABLE
coordinates	                            STRING	NULLABLE
num_oa_outputs	                        INTEGER	NULLABLE
num_in_doaj	                            INTEGER	NULLABLE
num_green_outputs	                    INTEGER	NULLABLE
num_gold_outputs	                    INTEGER	NULLABLE
num_gold_just_doaj_outputs	            INTEGER	NULLABLE
num_hybrid_outputs	                    INTEGER	NULLABLE
num_bronze_outputs	                    INTEGER	NULLABLE
num_green_only_outputs	                INTEGER	NULLABLE
num_green_only_ignoring_bronze_outputs	INTEGER	NULLABLE
num_has_license	                        INTEGER	NULLABLE
num_is_cclicensed	                    INTEGER	NULLABLE
citations	                            RECORD	NULLABLE
citations.mag	                        INTEGER	NULLABLE
citations.crosssref	                    INTEGER	NULLABLE
citations.open_citations	            INTEGER	NULLABLE
disciplines	                            RECORD	REPEATED

* Schema for disciplines captured above
*/
CREATE TEMP FUNCTION process_relations(relationships ANY TYPE, total INT64, total_oa INT64) as (
  ARRAY(SELECT AS STRUCT
          relation.identifier as id,
          COUNT(relation.identifier) as total_outputs,
          ROUND(SAFE_DIVIDE( COUNT(relation.identifier), total), 3) as percentage_of_all_outputs,
          ROUND(SAFE_DIVIDE( COUNTIF(unpaywall.is_oa) , total_oa ), 3) as percentage_of_all_oa,
          MAX(relation.name) as name,
          MAX(relation.country) as country,
          MAX(relation.country_code) as country_code,
          MAX(relation.region) as region,
          MAX(relation.subregion) as subregion,
          MAX(relation.coordinates) as coordinates,
          count_access_types(ARRAY_AGG(unpaywall)).*,
          STRUCT(
            SUM(citations.mag) as mag,
            SUM(citations.crossref) as crosssref,
            SUM(citations.open_citations) as open_citations
          ) as citations,
          group_disciplines(ARRAY_CONCAT_AGG(disciplines)) as disciplines
      FROM UNNEST(relationships) as relations 
      GROUP BY relation.identifier 
      ORDER BY total_outputs DESC
      LIMIT 1000)
);

-- Helper Function: Create a summary of events, grouped into the event type
/*
Output Schema:
source	                    STRING	NULLABLE
total_outputs	            INTEGER	NULLABLE
num_oa_outputs	            INTEGER	NULLABLE
num_green_outputs	        INTEGER	NULLABLE
num_gold_outputs	        INTEGER	NULLABLE
num_gold_just_doaj_outputs	INTEGER	NULLABLE
num_hybrid_outputs	        INTEGER	NULLABLE
num_bronze_outputs	        INTEGER	NULLABLE
num_green_only_outputs	    INTEGER	NULLABLE
*/
CREATE TEMP FUNCTION process_events(events ANY TYPE) as (
    ARRAY( 
    SELECT as STRUCT
        source,
        SUM(count) as total_outputs,
        SUM( IF(is_oa, count, 0) ) as num_oa_outputs, 
        SUM( IF(green, count, 0) ) as num_green_outputs, 
        SUM( IF(gold, count, 0) ) as num_gold_outputs,
        SUM( IF(gold_just_doaj, count, 0) ) as num_gold_just_doaj_outputs,
        SUM( IF(hybrid, count, 0) ) as num_hybrid_outputs,
        SUM( IF(bronze, count, 0) ) as num_bronze_outputs,
        SUM( IF(green_only, count, 0) ) as num_green_only_outputs
    FROM  UNNEST(events)
    GROUP BY source
    ORDER BY total_outputs DESC
  )
);

-- Helper Function: Creates a summary count for how many objects where hosted at a given repository
/*
Output Schema:
id	                            STRING	NULLABLE
total_outputs	                INTEGER	NULLABLE
percentage_of_all_green	FLOAT	NULLABLE
*/
CREATE TEMP FUNCTION process_repositories(repositories ARRAY<STRING>, total_green INT64) as (
  ARRAY(SELECT AS STRUCT
          repository as id,
          COUNT(repository) as total_outputs,
          ROUND(SAFE_DIVIDE( COUNT(repository), total_green), 3) as percentage_of_all_green,
      FROM UNNEST(repositories) as repository
      WHERE repository IS NOT NULL
      GROUP BY repository
      ORDER BY total_outputs DESC
      LIMIT 1000
   )
);

----
-- Main SQL Function
----
SELECT
  -- Common Metadata
  aggregrate.identifier as id,
  dois.crossref.{{ group_by_time_field }} as time_period,
  MAX(aggregrate.name) as name,
  MAX(aggregrate.country) as country,
  MAX(aggregrate.country_code) as country_code,
  MAX(aggregrate.country_code_2) as country_code_2,
  MAX(aggregrate.region) as region,
  MAX(aggregrate.subregion) as subregion,
  MAX(aggregrate.coordinates) as coordinates,

  -- Total outputs
  COUNT(dois.doi) as total_outputs,

  -- Access Types
  compute_access_types(
     ARRAY_AGG(
        STRUCT(
            dois.doi, STRUCT(dois.mag.CitationCount as mag, dois.crossref.references_count as crossref, dois.open_citations.citations_total as open_citations) as citations,
            unpaywall.is_oa, unpaywall.green, unpaywall.gold, unpaywall.gold_just_doaj, unpaywall.hybrid, unpaywall.bronze, unpaywall.green_only
        )
     )
  ) as access_types,
  
  -- Citations
  compute_citations(
     ARRAY_AGG(
        STRUCT(
            dois.doi, STRUCT(dois.mag.CitationCount as mag, dois.crossref.references_count as crossref, dois.open_citations.citations_total as open_citations) as citations
        )
     )
  ) as citations,

  -- Output Types
  count_output_types(
     ARRAY_AGG(
        STRUCT(
            unpaywall.output_type, STRUCT(dois.mag.CitationCount as mag, dois.crossref.references_count as crossref, dois.open_citations.citations_total as open_citations) as citations,
            unpaywall.is_oa, unpaywall.green, unpaywall.gold, unpaywall.gold_just_doaj, unpaywall.hybrid, unpaywall.bronze, unpaywall.green_only
        )
     )
  ) as output_types,

  -- Discipline
  /*
  The key thing to understand about the logic below is all about building input that is passed into the compute_disciplines function

  This whole block of SQL lives inside the 'GROUP BY aggregrate.identifier, crossref.{{ group_by_time_field }}' so it must include an aggregation
  The ARRAY_CONCAT_AGG performs this task. The reason it is not just an ARRAY_AGG, is because it is aggregrating multiple arrays,
  These are each created from the 'UNNEST(dois.mag.fields.level_0) as fields'

  Ultimately, what is passed into the compute_disciplines function is an array of STRUCTS, with each struct keyed off an individual fieldName
  */
  STRUCT(
    compute_disciplines( ARRAY_CONCAT_AGG( ARRAY(
      (SELECT AS STRUCT
          fields.DisplayName, fields.Score,
          STRUCT(
            dois.mag.CitationCount as mag,
            dois.crossref.references_count as crossref,
            dois.open_citations.citations_total as open_citations
          ) as citations,
          unpaywall.is_oa as is_oa, unpaywall.green as green, unpaywall.gold as gold, unpaywall.gold_just_doaj, unpaywall.hybrid, unpaywall.bronze, unpaywall.green_only,
          -- Total Funding
          (SELECT COUNT(funder) > 0 from UNNEST(affiliations.funders) as funder) as funding,
          -- Domestic, international, both, none or unknown funding
          (SELECT COUNT(funder) > 0 from UNNEST(affiliations.funders) as funder WHERE funder.country_code <> aggregrate.country_code_2) as international_funding,
          (SELECT COUNT(funder) > 1 from UNNEST(affiliations.funders) as funder WHERE funder.country_code = aggregrate.country_code_2) as domestic_funding,
          -- Has Industry or Government or both funding
          (SELECT COUNT(funder) > 0 from UNNEST(affiliations.funders) as funder WHERE funder.funding_body_subtype = 'gov') as government_funding,
          (SELECT COUNT(funder) > 0 from UNNEST(affiliations.funders) as funder WHERE funder.funding_body_subtype = 'pri') as private_funding,
          -- Domestic collaboration only or international collaboration or single institution only
          (SELECT COUNT(collab) > 0 FROM UNNEST(affiliations.countries) as collab WHERE collab.country <> aggregrate.country) as international_collab
        FROM UNNEST(dois.mag.fields.level_0) as fields)) )) as level0
  ) as disciplines,

  -- Repoistories
  /*
  This SQL block aggregrates the repository names, remebering we are inside a larger GROUP BY statement,
  So what is being passed into the process_repositories function is a list of repository names and a count of total green for a given GROUP in the GROUP BY clause
  */
  process_repositories(
    ARRAY_CONCAT_AGG(
      dois.unpaywall.repository_names
    ), COUNTIF( unpaywall.green = True )
  ) as repositories,

  {% if relate_to_institutions %}
  -- Institutions
  /*
  This SQL block UNNESTS the 'UNNEST(affiliations.institutions)' section of the DOI table. By doing this, it creates a new rows for each institution listed
  The WHERE statement 'relation.identifier <> aggregrate.identifier' ignores the identifer that is the subject of the overall GROUP BY statement
  The aggregrate.identifer is only relevant where this is creating the institutions table, as this code is used to create a range of dervied tables

  The end goal is to pass on a list of objects to the process_relations function, which is able to compute summaries for each identified insitution
  */
  process_relations(
    ARRAY_CONCAT_AGG( 
      ARRAY(
        (SELECT as STRUCT 
          relation, 
          unpaywall,
          STRUCT(dois.mag.CitationCount as mag, dois.crossref.references_count as crossref, dois.open_citations.citations_total as open_citations) as citations,
          ARRAY( SELECT as STRUCT DisplayName, Score, unpaywall.is_oa FROM UNNEST(dois.mag.fields.level_0)) as disciplines
        FROM UNNEST(affiliations.institutions) as relation
        WHERE relation.identifier <> aggregrate.identifier) 
      )
    ), COUNT(dois.doi), COUNTIF(unpaywall.is_oa = True)
  ) as institutions,
  {% endif %}

  {% if relate_to_countries %}
  -- Countries
  /*
  This SQL block UNNESTS the 'UNNEST(affiliations.countries)' section of the DOI table. By doing this, it creates a new rows for each country listed
  The WHERE statement 'relation.identifier <> aggregrate.identifier' ignores the identifer that is the subject of the overall GROUP BY statement
  The aggregrate.identifer is only relevant where this is creating the country table, as this code is used to create a range of dervied tables

  The end goal is to pass on a list of objects to the process_relations function, which is able to compute summaries for each country insitution
  */
  process_relations(
    ARRAY_CONCAT_AGG( 
      ARRAY(
        (SELECT as STRUCT 
          relation, 
          unpaywall,
          STRUCT(dois.mag.CitationCount as mag, dois.crossref.references_count as crossref, dois.open_citations.citations_total as open_citations) as citations,
          ARRAY( SELECT as STRUCT DisplayName, Score, unpaywall.is_oa FROM UNNEST(dois.mag.fields.level_0)) as disciplines
        FROM UNNEST(affiliations.countries) as relation
        WHERE relation.identifier <> aggregrate.country_code OR aggregrate.country_code IS NULL) 
      )
    ), COUNT(dois.doi), COUNTIF(unpaywall.is_oa = True)
  ) as countries,
  {% endif %}

  {% if relate_to_groups %}
  -- Groupings
  /*
  This SQL block UNNESTS the 'UNNEST(affiliations.groupings)' section of the DOI table. By doing this, it creates a new rows for each group listed
  The WHERE statement 'relation.identifier <> aggregrate.identifier' ignores the identifer that is the subject of the overall GROUP BY statement
  The aggregrate.identifer is only relevant where this is creating the groups table, as this code is used to create a range of dervied tables

  The end goal is to pass on a list of objects to the process_relations function, which is able to compute summaries for each identified insitution
  */
  process_relations(
    ARRAY_CONCAT_AGG( 
      ARRAY(
        (SELECT as STRUCT 
          relation, 
          unpaywall,
          STRUCT(dois.mag.CitationCount as mag, dois.crossref.references_count as crossref, dois.open_citations.citations_total as open_citations) as citations,
          ARRAY( SELECT as STRUCT DisplayName, Score, unpaywall.is_oa FROM UNNEST(dois.mag.fields.level_0)) as disciplines
        FROM UNNEST(affiliations.groupings) as relation
        WHERE relation.identifier <> aggregrate.identifier) 
      )
    ), COUNT(dois.doi), COUNTIF(unpaywall.is_oa = True)
  ) as groupings,
  {% endif %}

  {% if relate_to_funders %}
  -- Funders
  /*
  This SQL block UNNESTS the 'UNNEST(affiliations.funders)' section of the DOI table. By doing this, it creates a new rows for each funder listed
  The WHERE statement 'relation.identifier <> aggregrate.identifier' ignores the identifer that is the subject of the overall GROUP BY statement
  The aggregrate.identifer is only relevant where this is creating the funders table, as this code is used to create a range of dervied tables

  The end goal is to pass on a list of objects to the process_relations function, which is able to compute summaries for each identified funder
  */
  process_relations(
    ARRAY_CONCAT_AGG( 
      ARRAY(
        (SELECT as STRUCT 
          relation, 
          unpaywall,
          STRUCT(dois.mag.CitationCount as mag, dois.crossref.references_count as crossref, dois.open_citations.citations_total as open_citations) as citations,
          ARRAY( SELECT as STRUCT DisplayName, Score, unpaywall.is_oa FROM UNNEST(dois.mag.fields.level_0)) as disciplines
        FROM UNNEST(affiliations.funders) as relation
        WHERE relation.identifier <> aggregrate.identifier) 
      )
    ), COUNT(dois.doi), COUNTIF(unpaywall.is_oa = True)
  ) as funders,
  {% endif %}

  {% if relate_to_members %}
  -- Members
  /*
  This SQL block UNNESTS the 'UNNEST(affiliations.members)' section of the DOI table. By doing this, it creates a new rows for each member listed
  Members are only relevant for generating the country and groups tables, as both of these types have 'members' that make up the whole

  The end goal is to pass on a list of objects to the process_relations function, which is able to compute summaries for each identified member
  */
  process_relations(
    ARRAY_CONCAT_AGG( 
      ARRAY(
        (SELECT as STRUCT 
          STRUCT(
                relation as identifier, relation as name, CAST(NULL as STRING) as country, CAST(NULL as STRING) as country_code,
                CAST(NULL as STRING) as region, CAST(NULL as STRING) as subregion, CAST(NULL as STRING) as coordinates
          ) as relation,
          unpaywall,
          STRUCT(
                dois.mag.CitationCount as mag, dois.crossref.references_count as crossref, dois.open_citations.citations_total as open_citations
          ) as citations,
          ARRAY( SELECT as STRUCT DisplayName, Score, unpaywall.is_oa FROM UNNEST(dois.mag.fields.level_0)) as disciplines
        FROM UNNEST(aggregrate.members) as relation) 
      )
    ), COUNT(dois.doi), COUNTIF(unpaywall.is_oa = True)
  ) as members,
  {% endif %}

  {% if relate_to_publishers %}
  -- Publishers
  /*
  This SQL block UNNESTS the 'UNNEST(affiliations.publishers)' section of the DOI table. By doing this, it creates a new rows for each publihser listed
  The WHERE statement 'relation.identifier <> aggregrate.identifier' ignores the identifer that is the subject of the overall GROUP BY statement
  The aggregrate.identifer is only relevant where this is creating the publishers table, as this code is used to create a range of dervied tables

  The end goal is to pass on a list of objects to the process_relations function, which is able to compute summaries for each identified publisher
  */
  process_relations(
    ARRAY_CONCAT_AGG( 
      ARRAY(
        (SELECT as STRUCT 
          relation, 
          unpaywall,
          STRUCT(dois.mag.CitationCount as mag, dois.crossref.references_count as crossref, dois.open_citations.citations_total as open_citations) as citations,
          ARRAY( SELECT as STRUCT DisplayName, Score, unpaywall.is_oa FROM UNNEST(dois.mag.fields.level_0)) as disciplines
        FROM UNNEST(affiliations.publishers) as relation
        WHERE relation.identifier <> aggregrate.identifier) 
      )
    ), COUNT(dois.doi), COUNTIF(unpaywall.is_oa = True)
  ) as publishers,
  {% endif %}

  {% if relate_to_journals %}
  -- Journals
  /*
  This SQL block UNNESTS the 'UNNEST(affiliations.journals)' section of the DOI table. By doing this, it creates a new rows for each journal listed
  The WHERE statement 'relation.identifier <> aggregrate.identifier' ignores the identifer that is the subject of the overall GROUP BY statement
  The aggregrate.identifer is only relevant where this is creating the journals table, as this code is used to create a range of dervied tables

  The end goal is to pass on a list of objects to the process_relations function, which is able to compute summaries for each identified journal
  */
  process_relations(
    ARRAY_CONCAT_AGG( 
      ARRAY(
        (SELECT as STRUCT 
          relation, 
          unpaywall,
          STRUCT(dois.mag.CitationCount as mag, dois.crossref.references_count as crossref, dois.open_citations.citations_total as open_citations) as citations,
          ARRAY( SELECT as STRUCT DisplayName, Score, unpaywall.is_oa FROM UNNEST(dois.mag.fields.level_0)) as disciplines
        FROM UNNEST(affiliations.journals) as relation
        WHERE relation.identifier <> aggregrate.identifier) 
      )
    ), COUNT(dois.doi), COUNTIF(unpaywall.is_oa = True)
  ) as journals,
  {% endif %}

  -- Events
  /*
  This SQL block UNNESTS the 'UNNEST(dois.events.events)' section of the DOI table.
  It concatinates all the individual event types and counts, along with the OA status of the DOIs those events are linked too
  The end goal of this block is to pass the resulting list into the process_events function to create the summary you see in the final output
  */
  process_events(
    ARRAY_CONCAT_AGG( 
      ARRAY((
        SELECT AS STRUCT 
          event.source, 
          event.count, 
          STRUCT(dois.mag.CitationCount as mag, dois.crossref.references_count as crossref, dois.open_citations.citations_total as open_citations) as citations, 
          unpaywall.is_oa as is_oa, unpaywall.green as green, unpaywall.gold as gold, unpaywall.gold_just_doaj, unpaywall.hybrid, unpaywall.bronze, unpaywall.green_only 
        FROM UNNEST(dois.events.events) as event)))
  ) as events,

FROM {{ doi_ref }} as dois, UNNEST(dois.affiliations.{{ aggregation_field }}) as aggregrate
WHERE aggregrate.identifier IS NOT NULL
GROUP BY aggregrate.identifier, crossref.{{ group_by_time_field }}
{% endmacro %}