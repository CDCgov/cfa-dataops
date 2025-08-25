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

## ETL and Versioning Patterns Supported

### Standard ETL

<!-- Add this anywhere in your Markdown file -->
<script type="module">
  import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
  mermaid.initialize({ startOnLoad: true });
</script>

<pre class="mermaid" style="background-color:#fff8e1">
  graph LR
  A(Source Data) --> B[Extract]
  B --> C[Transform]
  C --> D[Load]
  D --> E(Versioned Load Dataset)
  B --> F(Versioned Extract Dataset)
  style A fill:#f9f
  style E fill:#bbf
  style F fill:#bbf
</pre>

---

### Medallion

<pre class="mermaid" style="background-color:#fff8e1">
  graph LR
  A(Source Data) --> B[Extract/Bronze Transform]
  B --> C[Silver Transform]
  C --> D[Gold Transform]
  B --> E[Load]
  E --> F(Versioned Bronze/Extract Dataset)
  C --> G[Load]
  G --> H(Versioned Silver Dataset)
  D --> I[Load]
  I --> J(Versioned Gold Dataset)
  style A fill:#f9f
  style B fill:#b87333
  style C fill:#c0c0c0
  style D fill:#ffd700
  style F fill:#bbf
  style H fill:#bbf
  style J fill:#bbf
</pre>

---

### Source Data changes

<pre class="mermaid" style="background-color:#fff8e1">
  graph LR
  A(Source Data) --> B[Extract]
  B --> C[Schema Check]
  C -->|Schema Valid| D[Transform]
  C -->|Schema Valid| I(Versioned Extract Dataset)
  C -->|Schema Changed| E[Version Update<br>Note: May require new config<br>or schema update with comment in config]
  D --> G[Load]
  G --> H(Versioned Load Dataset)
  style A fill:#f9f
  style H fill:#bbf
  style I fill:#bbf
  style C fill:#ffa500
  style E fill:#ff6347
</pre>

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
