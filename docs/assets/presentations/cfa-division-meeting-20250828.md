---
marp: true
title: CFA-DataOps
theme: gaia
_class: invert
style: |
  section {
    font-size: 30px; /* Adjust the pixel value as needed */
  }
---

# cfa-dataops

Patterns for organizing and versioning datasets.

*Presenter: Phil Rogers (ap66)*

---

## Motivation

Datasets from data.cdc.gov are a key resource to the work that is performed in CFA, but those datasets can be changed without notice.

With the right set of patterns, we can:

- Store our own **versioned copies** of source data
- **Reduce redundant dataset** sourcing and duplicate ETL
- Foster data **operation transparency**
- Support sourcing data **beyond data.cdc.gov**
- Create **low friction access** to data users/modelers

---

##  Data as a Starting Point

In addition to organizing and managing ETL, the cfa-dataops seemed a fine starting point to do other data operations. Other features have been added that have a place throughout the data science pipeline. These include:

- Data schema validation with pandera, **handy for alerting** when source datasets have changed
- Synthetic test datasets for ETL workflow **testing and prototyping**
- **Project specific workflows** that go beyond the standard ETL structure
- Parameterized HTML **report generation** using Jupyter notebook as an authoring environment
- Some shared **data visualization patterns**

---

## Roadmap

- Add logs/ledger for data access (know when datasets go stale)
- ETL automation and scheduling
- Config inheritance
- Splitting the interpreter code from the configuration and workflow scripts
