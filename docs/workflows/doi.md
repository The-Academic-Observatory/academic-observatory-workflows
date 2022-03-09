# DOI Workflow

The primary purpose of the DOI workflow is to link together open datasets of higher education through the use of persistent identifiers, and to then produce useful summary statistics across a variety of common aggregations

## Input datasets

A number of specialised research publication datasets are collected. These include Crossref Metadata, Crossref Funder Registry, Crossref Events, Microsoft Academic Graph, Unpaywall, the Research Organization Registry, Open Citations. The table below illustrates each of these datesets and the PIDs that are exposed within them.

PIDs in this case refer to persistent identifiers. Commonly used identifier schemas that normalised how we refer to unique entities across a range of independent systems.

```eval_rst
+-------------------+-----------------------------------------------------+------------+
| Name              | Table                                               | PID        |
+===================+=====================================================+============+
| Crossref Metadata | `crossref.crossref_metadataYYYYMMDD`                | DOI        |
+-------------------+-----------------------------------------------------+------------+
| Crossref Events   | `crossref.crossref_events`                          | DOI        |
+-------------------+-----------------------------------------------------+------------+
| Crosref Fundref   | `crossref.crossref_fundrefYYYYMMDD`                 | FunderDOI  |
+-------------------+-----------------------------------------------------+------------+
| RoR               | `ror.rorYYYYMMDD`                                   | ROR/Country|
+-------------------+-----------------------------------------------------+------------+
| MAG               | `mag.*YYYYMMDD` (multiple individual tables)        | GRID/DOI   |
+-------------------+-----------------------------------------------------+------------+
| Open Citations    | `open_citations.open_citationsYYYYMMDD`             | DOI        |
+-------------------+-----------------------------------------------------+------------+
| ORCID             | `orcid.orcid`                                       | ORCID/DOI  |
+-------------------+-----------------------------------------------------+------------+
| Unpaywall         | `our_research.unpaywall`                            | DOI        |
+-------------------+-----------------------------------------------------+------------+
| Country & Region  | `settings.country`                                  | Country    |
| Names & Codes     |                                                     |            |
+-------------------+-----------------------------------------------------+------------+
```

## Synthesis

After fetching the datasets, they are synthesised to produce aggregate time series statistics for each country and institution (entity type) in the dataset. The aggregate timeseries statistics include publication count, open access status, citation count and alt-metrics. 

The synthesis occurs in three steps: 
- Creating a table of publications.
- Grouping the publications by entity type and year of publication.
- Computing aggregated summaries for each group. Each step of the process is explained below with examples.

``` eval_rst
.. image:: ../static/diagrams/workflow_summary.png
   :width: 650
```

The table of publications is created by joining records from the research publication datasets on Digital Object Identifiers (DOIs); unique digital identifiers given to the majority of publications. Figure 2 illustrates how each dataset contributes to the publications table during the joining process, using the example of a single publication. Unique publications are discovered with Crossref Metadata, from which the publication’s DOI, Journal, Publisher, Funder identifiers and citation counts are derived. The publication’s Open Access status is computed using Unpaywall. The authors of the paper and their institutional affiliations are derived with Microsoft Academic Graph. The Research Organisation Registry (ROR) is used to enrich the institutional affiliation records with institution details and iso3166 maps institutions to countries and regions.  

``` eval_rst
.. image:: ../static/diagrams/pid_workflow.png
   :width: 600
```

Once the publications table has been created, the publications are grouped by entity type and publication year. For instance publications are grouped by institution and publication year. The last step involves creating aggregate timeseries statistics based on the yearly groups of publications.

``` eval_rst
.. image:: ../static/diagrams/aggregate_publications.png
   :width: 650
```

### Intermediate Datasets

Digging into the mechanics of the workflow, there are a number of intermediate tables that are created. Much of the time, these are not relevant as the information is better organised in the final output tables. However, for specific analysis they may be useful so are included here for completeness 

The primary purpose of these tables is to pre-process some of the raw datasets, creating a new format that is easier to work with downstream. In the case of GRID, it adds additional information around county and region from the iso3166 dataset, and for unpaywall it computes some COKI defined logic for OA status types.

For each of the scripts, they can be found in the following [folder](https://github.com/The-Academic-Observatory/academic-observatory-workflows/tree/develop/academic_observatory_workflows/database/sql)

```eval_rst
+----------------+----------------------------------------------------+-----+-----------------------------------+
| Name           | Table                                              | PID | SQL File Name                     |
+================+====================================================+=====+===================================+
| Crossref Events| `observatory_intermediate.crossref_eventsYYYYMMDD` | DOI | create_crossref_events.sql.jinja2 |
+----------------+----------------------------------------------------+-----+-----------------------------------+
| Crosref Fundref| `observatory_intermediate.crossref_fundrefYYYYMMDD`| DOI | create_crossref_fundref.sql.jinja2|
+----------------+----------------------------------------------------+-----+-----------------------------------+
| RoR            | `observatory_intermediate.rorYYYYMMDD`             | ROR | create_ror.sql.jinja2             |
+----------------+----------------------------------------------------+-----+-----------------------------------+
| MAG            | `observatory_intermediate.magYYYYMMDD`             | DOI | create_mag.sql.jinja2             |
+----------------+----------------------------------------------------+-----+-----------------------------------+
| Open Citations | `observatory_intermediate.open_citationsYYYYMMDD`  | DOI | create_open_citations.sql.jinja2  |
+----------------+----------------------------------------------------+-----+-----------------------------------+
| ORCID          | `observatory_intermediate.orcidYYYYMMDD`           | DOI | create_orcid.sql.jinja2           |
+----------------+----------------------------------------------------+-----+-----------------------------------+
| Unpaywall      | `observatory_intermediate.unpaywallYYYYMMDD`       | DOI | create_unpaywall.sql.jinja2       |
+----------------+----------------------------------------------------+-----+-----------------------------------+

```

### Output datasets

The final set of output dataset are as follows. Aside from the DOI table, which is an integrated set of all the input datasets linked by DOI, they each share a very similar, or identical, schema. The difference is around the entity being aggregated against.

```eval_rst
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Name            | Table                                       | PID       | SQL File Name                   |
+=================+=============================================+===========+=================================+
| DOI             | `observatory.doiYYYYMMDD`                   | DOI       | create_doi.sql.jinja2           |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Author          | `observatory.authorYYYYMMDD`                | ORCID     | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Book            | `observatory.bookYYYYMMDD`                  | ISBN      | create_book.sql.jinja2          |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Country         | `observatory.countryYYYYMMDD`               | Alpha3    | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Funder          | `observatory.funderYYYYMMDD`                | Name      | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Group           | `observatory.groupYYYYMMDD`                 | Name      | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Institution     | `observatory.institutionYYYYMMDD`           | ROR       | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Journal         | `observatory.journalYYYYMMDD`               | ISSN-l    | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Publisher       | `observatory.publisherYYYYMMDD`             | Name      | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Region          | `observatory.regionYYYYMMDD`                | Region    | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Sub-region      | `observatory.subregionYYYYMMDD`             | SubRegion | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
```

### Output Schemas

The following Links provide details in to the schemas behind the doi, book and various other aggregration tables schemas:
- [Doi table](./doi_output_schema.md)
- [Books table](./book_output_schema.md)
- [Shared Schema for Aggregration Output tables](./aggregate_output_schema.md)

## Other Outputs

There are also two additional output from this workflow

### COKI Dashboards

This dataset is a copy of the observatory output datasets, with two differences.
- Firstly, the table naming removes the trailing date. It always replaces the current version with the new version each week. By keeping the names consistent, datastudio dashboards can reference this stable name, and continue to review updates
- Secondly, it adds a range of database 'views'. These views are created to enable comparisons in Data Studio. Providing a secondary table to reference, which keeps only the bare minimum information for each table being compared. This is the common 'view' [query](https://github.com/The-Academic-Observatory/academic-observatory-workflows/blob/develop/academic_observatory_workflows/database/sql/comparison_view.sql.jinja2) that is shared across these comparison views.

### Data Exports

This dataset contains a range of tables that are exported specifically for Elasticsearch/Kibana. Because of the limitations within Kibana Dashboards, the output tables needs to be further broken down into individual sections. The follow table provides the list of queries that produce this output.

The way to understand each of the below queries, as that each of them (except the dois one) are run for every output table. For for example, where the table below says `data_export.ao_*_access_typesYYYYMMDD` this will result in a range of table, one of which will take the form `data_export.ao_author_access_typesYYYYMMDD`.

A final note, for the 'Relations' output query applies to a range of exports, due to them all sharing the same schema. Those types are:
- Countries
- Funders
- Groupings
- Institutions
- Journals
- Publishers

For each of the scripts, they can be found in the following [folder](https://github.com/The-Academic-Observatory/academic-observatory-workflows/tree/develop/academic_observatory_workflows/database/sql)

```eval_rst
+-----------------+---------------------------------------------+---------------------------------+
| Name            | Table                                       | SQL File Name                   |
+=================+=============================================+=================================+
| Access Types    | `data_export.ao_*_access_typesYYYYMMDD`     | export_access_types.sql.jinja2  |
+-----------------+---------------------------------------------+---------------------------------+
| Disciplines     | `data_export.ao_*_disciplinesYYYYMMDD`      | export_disciplines.sql.jinja2   |
+-----------------+---------------------------------------------+---------------------------------+
| Events          | `data_export.ao_*_eventsYYYYMMDD`           | export_events.sql.jinja2        |
+-----------------+---------------------------------------------+---------------------------------+
| Metrics         | `data_export.ao_*_metricsYYYYMMDD`          | export_metrics.sql.jinja2       |
+-----------------+---------------------------------------------+---------------------------------+
| Output Types    | `data_export.ao_*_output_typesYYYYMMDD`     | export_output_types.sql.jinja2  |
+-----------------+---------------------------------------------+---------------------------------+
| Relations       | `data_export.ao_*_[relation]YYYYMMDD`       | export_relations.sql.jinja2     |
+-----------------+---------------------------------------------+---------------------------------+
| Unique Lists    | `data_export.ao_*_unique_listYYYYMMDD`      | export_unique_list.sql.jinja2   |
+-----------------+---------------------------------------------+---------------------------------+
| Dois            | `data_export.ao_doisYYYYMMDD`               | export_dois.sql.jinja2          |
+-----------------+---------------------------------------------+---------------------------------+
```