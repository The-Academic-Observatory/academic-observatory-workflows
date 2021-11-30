# DOI Workflow

The primary purpose of the DOI workflow is to link together open datasets of higher education through the use of persistent identifiers, and to then produce useful summary statistics across a variety of common aggregations

## Input datasets

Each a number of specialised research publication datasets are collected. These include Crossref Metadata, Crossref Funder Registry, Crossref Events, Microsoft Academic Graph, Unpaywall, the Research Organization Registry, Open Citations and Geonames. The table below illustrates each of these datesets and the PIDs that are exposed within them.

```eval_rst
+-------------------+-----------------------------------------------------+-----------+
| Name              | Table                                               | PID       |
+===================+=====================================================+===========+
| Crossref Metadata | `crossref.crossref_metadataYYYYMMDD`                | DOI       |
+-------------------+-----------------------------------------------------+-----------+
| Crossref Events   | `crossref.crossref_events`                          | DOI       |
+-------------------+-----------------------------------------------------+-----------+
| Crosref Fundref   | `crossref.crossref_fundrefYYYYMMDD`                 | FunderDOI |
+-------------------+-----------------------------------------------------+-----------+
| Grid              | `digital_science.gridYYYYMMDD`                      | GRID      |
+-------------------+-----------------------------------------------------+-----------+
| MAG               | `mag.*YYYYMMDD` (multiple individual tables)        | GRID/DOI  |
+-------------------+-----------------------------------------------------+-----------+
| Open Citations    | `open_citations.open_citationsYYYYMMDD`             | DOI       |
+-------------------+-----------------------------------------------------+-----------+
| ORCID             | `orcid.orcid`                                       | ORCID/DOI |
+-------------------+-----------------------------------------------------+-----------+
| Unpaywall         | `our_research.unpaywall`                            | DOI       |
+-------------------+-----------------------------------------------------+-----------+
| iso3166           | `ios.iso3166_countries_and_regions`                 | Country   |
+-------------------+-----------------------------------------------------+-----------+
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

The table of publications is created by joining records from the research publication datasets on Digital Object Identifiers (DOIs); unique digital identifiers given to the majority of publications. Figure 2 illustrates how each dataset contributes to the publications table during the joining process, using the example of a single publication. Unique publications are discovered with Crossref Metadata, from which the publication’s DOI, Journal, Publisher, Funder identifiers and citation counts are derived. The publication’s Open Access status is computed using Unpaywall. The authors of the paper and their institutional affiliations are derived with Microsoft Academic Graph. The Research Organisation Registry (ROR) is used to enrich the institutional affiliation records with institution details and GeoNames maps institutions to countries and regions.  

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

```eval_rst
+----------------+----------------------------------------------------+-----+-----------------------------------+
| Name           | Table                                              | PID | SQL File Name                     |
+================+====================================================+=====+===================================+
| Crossref Events| `observatory_intermediate.crossref_eventsYYYYMMDD` | DOI | create_crossref_events.sql.jinja2 |
+----------------+----------------------------------------------------+-----+-----------------------------------+
| Crosref Fundref| `observatory_intermediate.crossref_fundrefYYYYMMDD`| DOI | create_crossref_fundref.sql.jinja2|
+----------------+----------------------------------------------------+-----+-----------------------------------+
| Grid           | `observatory_intermediate.gridYYYYMMDD`            | GRID| create_grid.sql.jinja2            |
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

The final set of output dataset are as followed. Aside from the DOI table, which is an integrated set of all the input datasets linked by DOI, they each share a very similar, or identical, schema. The difference is around the entity being aggregated against.

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
| Institution     | `observatory.institutionYYYYMMDD`           | GRID      | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Journal         | `observatory.journalYYYYMMDD`               | ISSN-l    | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Publisher       | `observatory.publisherYYYYMMDD`             | Name      | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Region          | `observatory.regionYYYYMMDD`                | Region    | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Sub Region      | `observatory.subregionYYYYMMDD`             | SubRegion | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
```

## Testing

Overview of how the tests work here...