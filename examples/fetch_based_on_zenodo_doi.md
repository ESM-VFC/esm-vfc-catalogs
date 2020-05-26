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


## An example catalog

We want to have a catalog pointing to data on disk and having a Zenodo DOI as metadata.
Then, we want to be able to download all files from Zenodo that match files really needed in the catalog.

```python
%%file fesom2_catalog.yaml

metadata:
  version: 1

plugins:
  source:
      - module: intake_xarray

sources:

  FESOM2_sample:
    driver: netcdf
    description: 'FESOM2 Sample dataset'
    metadata:
      zenodo_doi: "10.5281/zenodo.3819896"
    args:
      # TODO: Can we use a glob-style pattern here? --> .../*.fesom.????.nc
      urlpath: 
        - "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/temp.fesom.1948.nc"
        - "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/salt.fesom.1948.nc"
        - "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/u.fesom.1948.nc"
        - "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/v.fesom.1948.nc"
        - "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/w.fesom.1948.nc"
        - "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/a_ice.fesom.1948.nc"
        - "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/m_ice.fesom.1948.nc"
        - "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/vice.fesom.1948.nc"
        - "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/uice.fesom.1948.nc"
        - "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/sst.fesom.1948.nc"
        - "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/ssh.fesom.1948.nc"
        - "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/MLD1.fesom.1948.nc"
        - "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/Kv.fesom.1948.nc"
        - "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/Av.fesom.1948.nc"
        - "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/vnod.fesom.1948.nc"
        - "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/unod.fesom.1948.nc"
      xarray_kwargs:
        decode_cf: False
        combine: 'by_coords'
```

## Imports and paths

```python
import intake
import requests
import pycurl
from urllib.parse import urlparse
import os
from pathlib import Path
import logging

import fnmatch
import hashlib
```

```python
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
```

```python
# parameters
data_path = Path("../esm_vfc_data/").resolve()
```

```python
os.environ["ESM_VFC_DATA_DIR"] = str(data_path)
```

## open catalog

```python
cat = intake.open_catalog("fesom2_catalog.yaml")
```

```python
list(cat)
```

```python
cat["FESOM2_sample"]
```

## How to pre-fetch the data?

We need something to check hashes.  And we want to download just based on Zenodo DOI and filename pattern as used in the catalog etrie's `urlpath`.

```python
def check_file(file_name, checksum, blocksize=65536):
    """Check if file satisfies checksum.
    
    Parameters
    ----------
    file_name : str | Path
        File name to check.
    checksum : str
        Format f"{algorithm}:{checksum}"
    blocksize : int
        Defaults to 65536 (Bytes).
        
    Returns
    -------
    bool : True if checksum matches.

    """  
    algorithm, target_hash = tuple(checksum.split(":"))
    file_hash = hashlib.new(algorithm)
    with open(file_name, "rb") as f:
        while True:
            chunk = f.read(blocksize)
            if not chunk:
                break
            file_hash.update(chunk)
    file_hash = file_hash.hexdigest()
    
    return file_hash == target_hash
```

```python
def download_zenodo_files(
    zenodo_doi,
    target_directory=None,
    force_download=False,
    filter_pattern=None
):
    """Download zenodo files for a given DOI.
    
    Parameters
    ----------
    zenodo_doi : str
        Zenodo DOI.  Example: "10.5281/zenodo.3819896"
    target_directory : path or str
        Target directory where all files will end up.
    force_download : bool
        Re-download and overwrite files even if they already exist?
    filter_pattern : str
        Pattern used to filter files.  Note that we use fnmatch and not regex.
        
    Returns
    -------
    list of paths : all target files.
    
    """    
    #     # check if we filter files
    #     if filter_files is not None:
    #         raise NotImplementedError("Filtering is not implemented yet")

    # get zenodo record ID from doi
    zenodo_record = zenodo_doi.split('.')[-1]
    logging.debug(f"will download record {zenodo_record}")
    
    # get full record from zenodo
    # see https://developers.zenodo.org/#quickstart-upload for pointers
    r = requests.get(f"https://zenodo.org/api/records/{zenodo_record}")
    logging.debug(f"got status code {r.status_code}")
    # should we debug-log the full json dump?

    # TODO: Check that we got the correct DOI
    
    # get list of source urls
    filtered_files = list(filter(
        lambda fn: fnmatch.fnmatch(fn["key"], filter_pattern),
        r.json()["files"]
    ))
    all_urls = [file["links"]["self"] for file in filtered_files]
    all_target_files = [
        Path(target_directory) / Path(parsed_url.path).name
        for parsed_url in map(urlparse, all_urls)
    ]
    all_checksums = [file["checksum"] for file in filtered_files]
    
    # ensure target dir exists
    Path(target_directory).mkdir(exist_ok=True, parents=True)
    
    # download all wanted files with curl
    for url, file, checksum in zip(all_urls, all_target_files, all_checksums):
        if not file.exists() or force_download:
            with open(file, "wb") as f:
                logging.debug(f"will download {url} to {file}")
                c = pycurl.Curl()
                c.setopt(c.URL, url)
                c.setopt(c.WRITEDATA, f)
                c.perform()
                c.close()
                logging.debug(f"download of {url} to {file} done")
        # This checks all files even if they were not downloaded:
        if not check_file(file_name=file, checksum=checksum):
            raise ValueError(f"Checksum for {file} does not match {checksum}")
    
    return all_target_files
```

## Download data

```python
download_zenodo_files(
    zenodo_doi=cat["FESOM2_sample"].metadata["zenodo_doi"],
    target_directory=Path(list(cat["FESOM2_sample"].urlpath)[0]).parent,
    force_download=False, 
    filter_pattern="*.fesom.????.nc"
)
```

```python
cat["FESOM2_sample"].to_dask()
```
