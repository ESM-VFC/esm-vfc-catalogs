---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.2
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# Automatically download FESOM2 data from Zenodo based on catalog


## Imports and paths

```python
import intake
import os
from pathlib import Path
from esmvfc_cattools import download_zenodo_files_for_entry
import logging
```

```python
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
```

```python
# parameters
data_path = Path("../esm_vfc_data/").resolve()
catalog_file = Path("../catalogs/FESOM2_PI_MESH.yaml")
```

```python
os.environ["ESM_VFC_DATA_DIR"] = str(data_path)
```

## Open catalog and download all data

```python
cat = intake.open_catalog(str(catalog_file))
```

```python
list(cat)
```

```python
for entry in [cat[name] for name in cat]:
    download_zenodo_files_for_entry(
        entry,
        force_download=False
    )
```

## Read catalog entries

```python
print(cat["FESOM2_sample"].read())
```

```python
cat["MESH_AUX3D"].read()
```

```python
print(cat["MESH_NOD2D"].read())
```

```python
print(cat["MESH_ELEM2D"].read())
```
