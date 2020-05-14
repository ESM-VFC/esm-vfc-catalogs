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

# PoC for fetching data from Zenodo based on a DOI

```python
import intake
import requests
import pycurl
from urllib.parse import urlparse
import os
from pathlib import Path
import logging
```

```python
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
```

```python
!pwd
```

```python
# parameters
catalog_file = "../catalogs/fesom2_catalog.yaml"
data_path = Path("../esm_vfc_data/").resolve()
```

```python
os.environ["ESM_VFC_DATA_DIR"] = str(data_path)
```

```python
cat = intake.open_catalog(catalog_file)
# cat["FESOM2_PIi_mesh_a_ice"].read()  # No data yet
```

```python
ACCESS_TOKEN="XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
```

```python
def download_zenodo_files(
    zenodo_doi, target_directory=None,
    force_download=False, filter_files=None):
    
    # check if we filter files
    if filter_files is not None:
        raise NotImplementedError("Filtering is not implemented yet")

    # get zenodo record ID from doi
    zenodo_record = zenodo_doi.split('.')[-1]
    logging.debug(f"will download record {zenodo_record}")
    
    # get full record from zenodo
    # see https://developers.zenodo.org/#quickstart-upload for pointers
    r = requests.get(
        f"https://zenodo.org/api/records/{zenodo_record}",
        params={'access_token': ACCESS_TOKEN}
    )
    logging.debug(f"got status code {r.status_code}")
    # should we debug-log the full json dump?

    # TODO: Check that we got the correct DOI
    
    # get list of source urls
    all_urls = [file["links"]["self"] for file in r.json()["files"]]
    all_target_files = [
        Path(target_directory) / Path(parsed_url.path).name
        for parsed_url in map(urlparse, all_urls)
    ]
    
    # ensure target dir exists
    Path(target_directory).mkdir(exist_ok=True, parents=True)
    
    # download all wanted files with curl
    for url, file in zip(all_urls, all_target_files):
        if not file.exists() or force_download:
            with open(file, "wb") as f:
                logging.debug(f"will download {url} to {file}")
                c = pycurl.Curl()
                c.setopt(c.URL, url)
                c.setopt(c.WRITEDATA, f)
                c.perform()
                c.close()
                logging.debug(f"download of {url} to {file} done")
    
    return all_target_files
```

```python
download_zenodo_files(
    zenodo_doi=cat["FESOM2_Pi_mesh_a_ice"].metadata["zenodo_doi"],
    target_directory=(data_path / "FESOM2_Pi_mesh")
)
```

```python
cat["FESOM2_Pi_mesh_a_ice"].read()
```
