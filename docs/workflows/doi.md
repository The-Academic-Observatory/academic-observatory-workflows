# DOI Workflow

The primary purpose of the DOI workflow is to link together open datasets of higher education through the use of persistent identifiers, and to then produce useful summary statistics across a variety of common aggregations

## Dataflow Digram

Insert diagram from LucidCharts here

## Input datasets

Description of these phase...

```eval_rst
+---------------------+------------------------------------------------------+---------------+---------------+
| Name                | Table                                                | Primary PID   | Query Link    |
+=====================+======================================================+===============+===============+
|                     |                                                      |               |               |
+---------------------+------------------------------------------------------+---------------+---------------+
```

## Intermedia datasets

Description of these phase...

```eval_rst
+------------------+----------------------------------------------------+--------+------------+
| Name             | Table                                               | PID   | Query Link |
+==================+=====================================================+=======+============+
| Crossref Events  | `observatory_intermediate.crossref_eventsYYYYMMDD`  | DOI   | [link](https://github.com/The-Academic-Observatory/academic-observatory-workflows/blob/develop/academic_observatory_workflows/database/sql/create_crossref_events.sql.jinja2)       |
+------------------+-----------------------------------------------------+-------+------------+
| Crosref Fundref  | `observatory_intermediate.crossref_fundrefYYYYMMDD` | DOI   |        |
+------------------+-----------------------------------------------------+-------+------------+
| Grid             | `observatory_intermediate.gridYYYYMMDD`             | GRID  |        |
+------------------+-----------------------------------------------------+-------+------------+
| MAG              | `observatory_intermediate.magYYYYMMDD`              | DOI   |        |
+------------------+-----------------------------------------------------+-------+------------+
| Open Citations   | `observatory_intermediate.open_citationsYYYYMMDD`   | DOI   |        |
+------------------+-----------------------------------------------------+-------+------------+
| ORCID            | `observatory_intermediate.orcidYYYYMMDD`            | DOI   |        |
+------------------+-----------------------------------------------------+-------+------------+
| Unpaywall        | `observatory_intermediate.unpaywallYYYYMMDD`        | DOI   |        |
+------------------+-----------------------------------------------------+-------+------------+

```

## Output datasets

Description of these phase...

```eval_rst
+---------------------+------------------------------------------------------+---------------+---------------+
| Name                | Table                                                | Primary PID   | Query Link    |
+=====================+======================================================+===============+===============+
|                     |                                                      |               |               |
+---------------------+------------------------------------------------------+---------------+---------------+
```

## Testing

Overview of how the tests work here...