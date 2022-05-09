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

Author: Richard Hosking, James Diprose 
*/

WITH dois_temp_table as (
SELECT
  *,
  (SELECT ARRAY(SELECT DISTINCT grid FROM UNNEST( IF(mag.grids IS NOT NULL, mag.grids, [])) AS grid)) as mag_grids
FROM
  (SELECT
    -- This SQL block links unpaywall, mag, open citations and crossref events to the DOI and the metadata found in the crossref metadata dataset
    UPPER(TRIM(ref.doi)) as doi,
    STRUCT(
            title, abstract, issued.date_parts[offset(0)] as published_year,
            CASE WHEN ARRAY_LENGTH(issued.date_parts) > 1 THEN issued.date_parts[offset(1)] ELSE 13 END as published_month,
            CONCAT(issued.date_parts[offset(0)], "-", CASE WHEN ARRAY_LENGTH(issued.date_parts) > 1 THEN issued.date_parts[offset(1)] ELSE 13 END) as published_year_month,
            type, ISSN, ISBN, issn_type, publisher_location, publisher, member, prefix, container_title, short_container_title, group_title, references_count,
            is_referenced_by_count, subject, published_print, license, volume, funder, page, author, link, clinical_trial_number, alternative_id
    ) as crossref,
    (SELECT as STRUCT * from {{ ref('unpaywall')  }} as oa WHERE oa.doi = UPPER(TRIM(ref.doi))) as unpaywall,
    (SELECT as STRUCT * from {{ ref('mag') }} as mag WHERE mag.doi = UPPER(TRIM(ref.doi))) as mag,
    (SELECT as STRUCT * from {{ ref('open_citations')  }} as oa WHERE oa.doi = UPPER(TRIM(ref.doi))) as open_citations,
    (SELECT as STRUCT * from {{ ref('crossref_events') }} as events WHERE events.doi = UPPER(TRIM(ref.doi))) as events
  FROM {{ source('crossref', 'crossref_metadata' ~ var('shard_date')) }} as ref
  WHERE ARRAY_LENGTH(issued.date_parts) > 0)
),

-- this query builds the .affiliation section of the final doi table. The primiary purpose of this is to allow the aggregrate_doi query
affiliations_temp_table as (
SELECT
  extras.doi as doi,

  institutions,

  /*
  The four affiliation types below (Country, SubRegion, Region and Grouping, take their values from the SubQuery Below 'LEFT JOIN( SELECT...' however they need to be GROUPed to ensure only unique lists.
  For example, two institutions in one country result in two records of the same counry and a squashed into a single instance
  */
  ARRAY(SELECT as STRUCT identifier, MAX(name) as name, ["Country"] as types, CAST(MAX(country) as STRING) as country, CAST(MAX(country_code) as STRING) as country_code,
        CAST(MAX(country_code_2) as STRING) as country_code_2, CAST(MAX(region) as STRING) as region, CAST(MAX(subregion) as STRING) as subregion, CAST(NULL as STRING) as coordinates,
        COUNT(*) as count, ARRAY_AGG(DISTINCT member_identifier IGNORE NULLS) as members FROM UNNEST(countries) GROUP BY identifier
  ) as countries,

  ARRAY(SELECT as STRUCT identifier, MAX(name) as name, ["Subregion"] as types, CAST(MAX(country) as STRING) as country, CAST(MAX(country_code) as STRING) as country_code,
        CAST(MAX(country_code_2) as STRING) as country_code_2, CAST(MAX(region) as STRING) as region, CAST(MAX(subregion) as STRING) as subregion, CAST(NULL as STRING) as coordinates,
        COUNT(*) as count, ARRAY_AGG(DISTINCT member_identifier IGNORE NULLS) as members  FROM UNNEST(subregions) GROUP BY identifier
  ) as subregions,

  ARRAY(SELECT as STRUCT identifier, MAX(name) as name, ["Region"] as types, CAST(MAX(country) as STRING) as country, CAST(MAX(country_code) as STRING) as country_code,
        CAST(MAX(country_code_2) as STRING) as country_code_2, CAST(MAX(region) as STRING) as region, CAST(MAX(subregion) as STRING) as subregion, CAST(NULL as STRING) as coordinates,
        COUNT(*) as count, ARRAY_AGG(DISTINCT member_identifier IGNORE NULLS) as members FROM UNNEST(regions) GROUP BY identifier
  ) as regions,

  ARRAY(SELECT as STRUCT grouping_entity.group_id as identifier, MAX(grouping_entity.group_name) as name, ["Grouping"] as types, CAST(NULL as STRING) as country,
        MAX(grouping_entity.country_code) as country_code, MAX(grouping_entity.country_code) as country_code_2, CAST(NULL as STRING) as region, CAST(NULL as STRING) as subregion,
        CAST(NULL as STRING) as coordinates, ARRAY_AGG(DISTINCT grid_group.member_identifier IGNORE NULLS) as members FROM UNNEST(ror_groups) as grid_group,
        UNNEST(grid_group.groupings) as grouping_entity GROUP BY grouping_entity.group_id
  ) as groupings,

  -- Funder
  /*
  Chooses the values that are passed along in the funder affiliations, the schema is matching all other affiliations to enable flexibility of later SQL templates.
  The values from JOINING the funder reference in Crossref metada with the Fundref dataset
  */
  ARRAY(SELECT as STRUCT funder.funder.name as identifier, funder.funder.name as name, funder.funder.doi as doi, ["Funder"] as types,
        CAST(funder.fundref.country as STRING) as country, CAST(funder.fundref.country_code as STRING) as country_code, CAST(NULL as STRING) as country_code_2,
        CAST(funder.fundref.region as STRING) as region, CAST(NULL as STRING) as subregion, CAST(NULL as STRING) as coordinates, funder.fundref.funding_body_type as funding_body_type,
        funder.fundref.funding_body_sub_type as funding_body_subtype, CAST([] AS ARRAY<STRING>) as members FROM UNNEST(fundref.funders) as funder
        ) as funders,

  -- Discipline
  /*
  Chooses the values that are passed along in the discipline affiliations, the schema is matching all other affiliations to enable flexibility of later SQL templates.
  The values come from the fields of study in the MAG dataset
  */
  ARRAY(SELECT as STRUCT field.DisplayName as identifier, field.DisplayName as name, ["Discipline"] as types,
        CAST(NULL as STRING) as country, CAST(NULL as STRING) as country_code, CAST(NULL as STRING) as country_code_2, CAST(NULL as STRING) as region,
        CAST(NULL as STRING) as subregion, CAST(NULL as STRING) as coordinates, CAST([] AS ARRAY<STRING>) as members FROM UNNEST(mag.fields.level_0) as field
        ) as disciplines,

  -- Authors
  /*
  Chooses the values that are passed along in the author affiliations, the schema is matching all other affiliations to enable flexibility of later SQL templates.
  The values come from the ORCID dataset, which is pivotted to index by DOI, which is the primary index in this table
  */
  ARRAY(SELECT as STRUCT author.uri as identifier, CONCAT(author.given_names, " ", author.family_name) as name, author.given_names as given_names,
        author.family_name as family_name,["Author"] as types, CAST(NULL as STRING) as country, CAST(NULL as STRING) as country_code,
        CAST(NULL as STRING) as country_code_2, CAST(NULL as STRING) as region, CAST(NULL as STRING) as subregion, CAST(NULL as STRING) as coordinates, CAST([] AS ARRAY<STRING>) as members
        FROM UNNEST(orcid.orcid) as author
        ) as authors,

  -- Journal
  /*
  While there is only ever one Journal for a pubication, this single struct is placed inside of an array '[ ]' to ensure it conforms to the same schema as other affiliation types.
  This enables templated queries downstream to be greatly simplified
  */
  [ STRUCT( unpaywall.journal_issn_l as identifier, unpaywall.normalised_journal_name as name, ["Journal"] as types, CAST(NULL as STRING) as country,
    CAST(NULL as STRING) as country_code, CAST(NULL as STRING) as country_code_2, CAST(NULL as STRING) as region, CAST(NULL as STRING) as subregion, CAST(NULL as STRING) as coordinates,
    CAST([] AS ARRAY<STRING>) as members )
   ] as journals,
  
  -- Publisher
  /*
  While there is only ever one Publisher for a pubication, this single struct is placed inside of an array '[ ]' to ensure it conforms to the same schema as other affiliation types.
  This enables templated queries downstream to be greatly simplified
  */
  [ STRUCT( crossref.publisher as identifier,crossref.publisher as name, ["Publisher"] as types, CAST(NULL as STRING) as country,
    CAST(NULL as STRING) as country_code, CAST(NULL as STRING) as country_code_2, CAST(NULL as STRING) as region, CAST(NULL as STRING) as subregion, CAST(NULL as STRING) as coordinates,
    CAST([] AS ARRAY<STRING>) as members)
  ] as publishers,
  
FROM
  dois_temp_table as extras
LEFT JOIN(
SELECT
    doi,
    -- Chooses the values that are passed along in the institutional affiliations, the schema is matching all other affiliations to enable flexibility of later SQL templates
    ARRAY_AGG(
      STRUCT(
        institution.id as identifier,
        institution.types as types,
        institution.name as name,
        institution.country.wikipedia_name as country,
        institution.country.alpha3 as country_code,
        institution.country.alpha2 as country_code_2,
        institution.country.region as region,
        institution.country.subregion as subregion,
        CONCAT(CAST(institution.addresses[SAFE_OFFSET(0)].lat as STRING), ", ", CAST(institution.addresses[SAFE_OFFSET(0)].lng as STRING)) as coordinates,
        CAST([] AS ARRAY<STRING>) as members
      )
    ) as institutions,

    -- Chooses the values that are passed along in the country affiliations, the schema is matching all other affiliations to enable flexibility of later SQL templates
    ARRAY_AGG(
      STRUCT(
        institution.country.alpha3 as identifier,
        institution.types as types,
        institution.country.wikipedia_name as name,
        institution.country.wikipedia_name as country,
        institution.country.alpha3 as country_code,
        institution.country.alpha2 as country_code_2,
        institution.country.region as region,
        institution.country.subregion as subregion,
        NULL as coordinates,
        institution.id as member_identifier
      )
    ) as countries,

    -- Chooses the values that are passed along in the subregional affiliations, the schema is matching all other affiliations to enable flexibility of later SQL templates
    ARRAY_AGG(
      STRUCT(
        institution.country.subregion as identifier,
        NULL as types,
        institution.country.subregion as name,
        NULL as country,
        NULL as country_code,
        NULL as country_code_2,
        institution.country.region as region,
        NULL as subregion,
        NULL as coordinates,
        institution.country.alpha3 as member_identifier
      )
    ) as subregions,

    -- Chooses the values that are passed along in the regional affiliations, the schema is matching all other affiliations to enable flexibility of later SQL templates
    ARRAY_AGG(
      STRUCT(
        institution.country.region as identifier,
        NULL as types,
        institution.country.region as name,
        NULL as country,
        NULL as country_code,
        NULL as country_code_2,
        institution.country.region as region,
        NULL as subregion,
        NULL as coordinates,
        institution.country.subregion as member_identifier
      )
    ) as regions,
    
    ARRAY_AGG(
      STRUCT(
        ror_groups.groupings as groupings,
        institution.id as member_identifier
      )
    ) as ror_groups,

  /*
   dois_temp_table is created as the first sub-query in this script. It contains the data from crossref, unpaywall, mag, open_citations and crossref events
   This is then LEFT JOINed against the grid dataset to take the raw GRID IDs that come from the MAG dataset, with a more detailed view of that institution and its location. The county and region information comes from here
   This Instituional information is JOINed against a COKI created dataset, groupings, which allows for arbitary grouping of institutions (or really GRIDs).

   Lastly, Fundref and ORCID are also LEFT JOINed in order to drive the Funder and Author relationships
  */
  FROM dois_temp_table as dois, UNNEST(mag_grids) as grid_id
  LEFT JOIN {{ ref('ror') }} as institution on grid_id = institution.external_ids.GRID.preferred
  LEFT JOIN (SELECT ror, ARRAY_AGG(STRUCT(group_id, group_name, country_code)) as groupings FROM {{ source('settings', 'groupings') }} CROSS JOIN UNNEST(rors) as ror GROUP BY ror) as ror_groups on institution.id = ror_groups.ror
  GROUP BY doi) as base on extras.doi = base.doi
  LEFT JOIN {{ ref('crossref_fundref') }} as fundref on fundref.doi = extras.doi
  LEFT JOIN {{ ref('orcid') }} as orcid on UPPER(TRIM(orcid.doi)) = UPPER(TRIM(extras.doi))
)

/*
Brings together the two temporary tables above.
- dois.* is the collective metadata linked to the doi in question
- affiliations is the derived data that neatly organises all the relationships a doi has to authors, their institutions, countries and regions of those institutions, publisher, journal, funders
*/
SELECT
  dois.* EXCEPT (mag_grids),
  affiliations
FROM dois_temp_table as dois
LEFT JOIN affiliations_temp_table as affiliations on affiliations.doi = dois.doi
