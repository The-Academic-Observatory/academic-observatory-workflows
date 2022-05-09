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

-- This query allows affiliation IDs recorded in MAG to be modified when problems are discovered
WITH affiliations_processed as (
  SELECT
    affiliation.AffiliationId,
    affiliation.DisplayName,
    IF(override.grid_id is not null, override.grid_id, affiliation.GridId) as GridId
  FROM {{ source('mag', 'Affiliations' ~ var('shard_date_mag')) }} as affiliation
  LEFT JOIN {{ source('settings', 'mag_affiliation_override') }} as override on override.AffiliationId = affiliation.AffiliationId
),

-- This query preprocesses the Fields of Study to simply the later queries
fields_of_study as (
    SELECT
        fields.*,
        ARRAY(SELECT AS STRUCT
            extended.AttributeType as AttributeType,
            extended.AttributeValue as AttributeValue
         FROM {{ source('mag', 'FieldOfStudyExtendedAttributes' ~ var('shard_date_mag')) }} as extended
         WHERE extended.FieldOfStudyId = fields.FieldOfStudyId
        ) as extended
    FROM {{ source('mag', 'FieldsOfStudy' ~ var('shard_date_mag')) }} as fields
)

-- Main Query that takes the range of normalised MAG tables and creates single, DOI indexed, table that is easier to join with Crossref
SELECT 
  papers.* EXCEPT (journalId, ConferenceSeriesId, ConferenceInstanceId, CreatedDate) REPLACE ( UPPER(TRIM(Doi)) AS Doi),
  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(JSON_EXTRACT(IndexedAbstract, '$.InvertedIndex'), "[0-9]+", ""), ":", ""), ",", " "), '"', ""), "{", ""), "}", ""), "[", ""), "]", "") as abstract,
  fields.fields,
  mesh.mesh,
  authors.authors,
  STRUCT(journal.JournalId, journal.DisplayName, journal.Issn, journal.Publisher) as journal,
  STRUCT(conferenceInstance.ConferenceInstanceId,
         conferenceInstance.NormalizedName,
         conferenceInstance.DisplayName,
         conferenceInstance.Location,
         conferenceInstance.OfficialUrl,
         conferenceInstance.StartDate,
         conferenceInstance.EndDate,
         conferenceInstance.PaperCount,
         conferenceInstance.Latitude,
         conferenceInstance.Longitude
  ) as conferenceInstance,
  STRUCT(conferenceSeries.ConferenceSeriesId, conferenceSeries.NormalizedName, conferenceSeries.DisplayName) as conferenceSeries,
  extended.attributes,
  resources.resources,
  urls.urls,
  ARRAY((SELECT GridId FROM authors.authors WHERE GridId IS NOT NULL GROUP BY GridID)) as grids
FROM (SELECT UPPER(TRIM(doi)), ARRAY_AGG(Paperid ORDER BY CitationCount DESC)[offset(0)] as PaperId
      FROM {{ source('mag', 'Papers' ~ var('shard_date_mag')) }} as papers
      WHERE (papers.FamilyId is null OR papers.FamilyId = papers.PaperId) AND papers.doi IS NOT NULL
      GROUP BY UPPER(TRIM(doi))) as dois

LEFT JOIN {{ source('mag', 'Papers' ~ var('shard_date_mag')) }} as papers ON papers.PaperId = dois.PaperId

-- Abstract
LEFT JOIN {{ source('mag', 'PaperAbstractsInvertedIndex' ~ var('shard_date_mag')) }} as abstracts ON abstracts.PaperId = papers.PaperId

-- Journal
LEFT JOIN {{ source('mag', 'Journals' ~ var('shard_date_mag')) }} as journal ON journal.JournalId = papers.JournalId

-- ConferenceInstance
LEFT JOIN {{ source('mag', 'ConferenceInstances' ~ var('shard_date_mag')) }} as conferenceInstance ON conferenceInstance.ConferenceInstanceId = papers.ConferenceInstanceId

-- ConferenceSeries
LEFT JOIN {{ source('mag', 'ConferenceSeries' ~ var('shard_date_mag')) }} as conferenceSeries ON conferenceSeries.ConferenceSeriesId = papers.ConferenceSeriesId

-- Fields of Study
LEFT JOIN (SELECT 
              papers.PaperId, 
              -- Fields of Study
              STRUCT(
              ARRAY_AGG(IF(fields.Level = 0, STRUCT(fields.DisplayName,fields.FieldOfStudyId,fields.Rank,fields.MainType,paperFields.Score,extended), null) IGNORE NULLS ORDER BY paperFields.Score DESC) as level_0,
              ARRAY_AGG(IF(fields.Level = 1, STRUCT(fields.DisplayName,fields.FieldOfStudyId,fields.Rank,fields.MainType,paperFields.Score,extended), null) IGNORE NULLS ORDER BY paperFields.Score DESC) as level_1,
              ARRAY_AGG(IF(fields.Level = 2, STRUCT(fields.DisplayName,fields.FieldOfStudyId,fields.Rank,fields.MainType,paperFields.Score,extended), null) IGNORE NULLS ORDER BY paperFields.Score DESC) as level_2,
              ARRAY_AGG(IF(fields.Level = 3, STRUCT(fields.DisplayName,fields.FieldOfStudyId,fields.Rank,fields.MainType,paperFields.Score,extended), null) IGNORE NULLS ORDER BY paperFields.Score DESC) as level_3,
              ARRAY_AGG(IF(fields.Level = 4, STRUCT(fields.DisplayName,fields.FieldOfStudyId,fields.Rank,fields.MainType,paperFields.Score,extended), null) IGNORE NULLS ORDER BY paperFields.Score DESC) as level_4,
              ARRAY_AGG(IF(fields.Level = 5, STRUCT(fields.DisplayName,fields.FieldOfStudyId,fields.Rank,fields.MainType,paperFields.Score,extended), null) IGNORE NULLS ORDER BY paperFields.Score DESC) as level_5) as fields
            FROM {{ source('mag', 'Papers' ~ var('shard_date_mag')) }}  as papers
            LEFT JOIN {{ source('mag', 'PaperFieldsOfStudy' ~ var('shard_date_mag')) }} as paperFields on papers.PaperId = paperFields.PaperId
            LEFT JOIN fields_of_study as fields on fields.FieldOfStudyId = paperFields.FieldOfStudyId
            WHERE papers.Doi IS NOT NULL
            GROUP BY papers.PaperId) as fields ON fields.PaperId = papers.PaperId

-- Authors
LEFT JOIN (SELECT 
              papers.PaperId, 
              ARRAY_AGG(STRUCT(paperAuthorAffiliations.AuthorSequenceNumber, paperAuthorAffiliations.AuthorID, paperAuthorAffiliations.OriginalAuthor, paperAuthorAffiliations.AffiliationId, paperAuthorAffiliations.OriginalAffiliation, affiliation.GridId, affiliation.DisplayName) IGNORE NULLS ORDER BY paperAuthorAffiliations.AuthorSequenceNumber ASC) as authors
            FROM {{ source('mag', 'Papers' ~ var('shard_date_mag')) }} as papers
            LEFT JOIN {{ source('mag', 'PaperAuthorAffiliations' ~ var('shard_date_mag')) }} as paperAuthorAffiliations on paperAuthorAffiliations.PaperId = papers.PaperId
            LEFT JOIN affiliations_processed as affiliation on affiliation.AffiliationId = paperAuthorAffiliations.AffiliationId
            GROUP BY papers.PaperId) as authors ON authors.PaperId = papers.PaperId

-- Extended Attributes
LEFT JOIN (SELECT
              PaperId,
              ARRAY_AGG(STRUCT( AttributeType, AttributeValue)) as attributes
            FROM {{ source('mag', 'PaperExtendedAttributes' ~ var('shard_date_mag')) }}
            GROUP BY PaperId) as extended ON extended.PaperId = papers.PaperId

-- Resources
LEFT JOIN (SELECT
              PaperId,
              ARRAY_AGG(STRUCT( ResourceType , ResourceUrl )) as resources
            FROM {{ source('mag', 'PaperResources' ~ var('shard_date_mag')) }}
            GROUP BY PaperId) as resources ON resources.PaperId = papers.PaperId

-- URLs
LEFT JOIN (SELECT
              PaperId,
              ARRAY_AGG(STRUCT( SourceType , SourceUrl, LanguageCode )) as urls
            FROM {{ source('mag', 'PaperUrls' ~ var('shard_date_mag')) }}
            GROUP BY PaperId) as urls ON urls.PaperId = papers.PaperId

-- PaperMESH
LEFT JOIN (SELECT
              PaperId,
              ARRAY_AGG(STRUCT( DescriptorUI,	DescriptorName,	QualifierUI,	QualifierName,	IsMajorTopic )) as mesh
            FROM {{ source('mag', 'PaperMeSH' ~ var('shard_date_mag')) }}
            GROUP BY PaperId) as mesh ON mesh.PaperId = papers.PaperId