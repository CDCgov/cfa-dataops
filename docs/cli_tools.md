# CLI Tools Reference

The cfa-dataops package provides several command-line tools for managing and accessing datasets. These tools make it easy to explore available datasets, check versions, and download data locally without writing any Python code.

## Available Commands

### `dataops_datasets` - List Available Datasets

Lists all datasets available across all installed catalogs.

**Basic Usage:**
```bash
dataops_datasets
```

**Output Example:**
```
Available Datasets:
- catalog1.dataset_a
- catalog1.dataset_b
- catalog2.dataset_x
- catalog2.dataset_y
```

**Filter by Prefix:**

You can filter the dataset list using the `--prefix` or `-p` option:

```bash
dataops_datasets --prefix catalog1
```

This will show only datasets that start with "catalog1".

---

### `dataops_stages` - View Dataset Stages

Shows all available stages for a specific dataset. The last stage in red is the default stage used when loading data.

**Usage:**
```bash
dataops_stages <dataset_namespace>
```

**Example:**
```bash
dataops_stages "catalog.my_dataset"
```

**Output Example:**
```
Stages for catalog.my_dataset:
- extract
- load  # will be in red

Note: Stages in red indicate the default stage for loading the dataset.
```

---

### `dataops_versions` - List Dataset Versions

Lists all available versions for a dataset stage, with the most recent version highlighted in red (default version).

**Basic Usage (uses default stage):**
```bash
dataops_versions <dataset_namespace>
```

**Example:**
```bash
dataops_versions "catalog.my_dataset"
```

**Specify a Stage:**

Use the `--stage` or `-s` option to view versions for a specific stage:

```bash
dataops_versions "catalog.my_dataset" --stage "extract"
```

**Output Example:**
```
catalog.my_dataset:
- 2025-10-31  # will be in red
- 2025-10-30
- 2025-10-29
```

The most recent version (at the top) is displayed in red, indicating it's the default.

---

### `dataops_save` - Download Data Locally

Downloads a specific dataset version to your local filesystem. This is useful for offline work, creating local caches, or working with data in external tools.

**Basic Usage:**
```bash
dataops_save <dataset_namespace> <local_directory>
```

**Example:**
```bash
dataops_save "catalog.my_dataset" "./data/my_dataset"
```

This downloads the latest version of the default stage to `./data/my_dataset`.

**Specify Stage and Version:**

```bash
dataops_save "catalog.my_dataset" "./data" --stage "load" --version "2025-10-30"
```

**Force Re-download:**

By default, if data already exists locally, it won't be re-downloaded. Use the `--force` or `-f` flag to force a re-download:

```bash
dataops_save "catalog.my_dataset" ./data --force
```

**Command Options:**
- `dataset`: (required) Full dataset namespace (e.g., `catalog.dataset_name`)
- `location`: (required) Local directory path where data will be saved (will be created if it doesn't exist)
- `--stage` or `-s`: (optional) Specific stage to download (defaults to the last stage)
- `--version` or `-v`: (optional) Specific version to download (defaults to the most recent)
- `--force` or `-f`: (optional) Force re-download even if data already exists locally

**Output Example:**
```
Dataset 'catalog.my_dataset' version '2025-10-31' at stage 'load' has been saved locally.

/home/user/data/my_dataset
├── file1.parquet
├── file2.parquet
└── metadata.json
```

---

## Common Workflows

### Exploring a New Catalog

1. **List all available datasets:**
   ```bash
   dataops_datasets
   ```

2. **Check stages for a dataset of interest:**
   ```bash
   dataops_stages "catalog.interesting_dataset"
   ```

3. **See what versions are available:**
   ```bash
   dataops_versions "catalog.interesting_dataset"
   ```

4. **Download the latest data:**
   ```bash
   dataops_save "catalog.interesting_dataset" "./local_data"
   ```

### Working with Multiple Catalogs

Filter datasets by catalog prefix:
```bash
dataops_datasets --prefix "public"
```

This helps when you have multiple catalogs installed and want to see what's available in a specific one.

### Refreshing Local Data

Force re-download to get the latest data:
```bash
dataops_save "catalog.dataset" "./data" --force
```

---

## Tips

- **Tab Completion**: Depending on your shell configuration, you may be able to use tab completion for dataset names
- **Help**: Add `--help` to any command to see its usage information
  ```bash
  dataops_datasets --help
  dataops_stages --help
  dataops_versions --help
  dataops_save --help
  ```
- **Directory Creation**: The `dataops_save` command automatically creates the target directory if it doesn't exist
- **Tree Display**: After downloading data, the command shows a tree view of the downloaded files for easy verification

---

## Error Handling

The CLI tools provide helpful error messages:

- **Invalid dataset name**: Shows list of available datasets
- **Invalid stage**: Shows list of available stages for that dataset
- **Invalid version**: Shows list of available versions for that stage
- **Permission errors**: Indicates if there are file system permission issues
- **Already downloaded**: Warns when data already exists (use `--force` to override)

---

## See Also

- [Data User Guide](data_user_guide.md) - For programmatic data access using Python
- [Managing Catalogs](managing_catalogs.md) - For setting up and managing data catalogs
- [Data Developer Guide](data_developer_guide.md) - For creating and managing datasets
