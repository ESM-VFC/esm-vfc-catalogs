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

## TODO: We'd like to only specify the zenodo_doi

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
      urlpath: "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/*.fesom.1948.nc"
      xarray_kwargs:
        decode_cf: False
        combine: 'by_coords'

  # CAUTION: The following is broken with current intake caching!
            
  MESH_NOD2D:
    driver: csv
    description: 'Node locations of sample FESOM pi mesh'
    metadata:
      zenodo_doi: "10.5281/zenodo.3819896"
    args:
      urlpath: "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/pi.tar.gz"
      csv_kwargs:
        delim_whitespace: True
        skiprows: 1
        names:
          - "node_number"
          - "x"
          - "y"
          - "flag"
    cache:
      - type: compressed
        decomp: tgz
        argkey: urlpath
        regex_filter: 'nod2d.out'

  MESH_ELEM2D:
    driver: csv
    description: 'Element locations of sample FESOM pi mesh'
    metadata:
      zenodo_doi: "10.5281/zenodo.3819896"
    args:
      urlpath: "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/pi.tar.gz"
      csv_kwargs:
        delim_whitespace: True
        skiprows: 1
        names:
          - "first_elem"
          - "second_elem"
          - "third_elem"
    cache:
      - type: compressed
        decomp: tgz
        argkey: urlpath
        regex_filter: 'elem2d.out'
            
  MESH_AUX3D:
    driver: csv
    description: 'Topography of sample FESMOM pi mesh'
    metadata:
      zenodo_doi: "10.5281/zenodo.3819896"
    args:
      urlpath: "{{env('ESM_VFC_DATA_DIR')}}/FESOM2_PI_MESH/pi.tar.gz"
      csv_kwargs:
        delim_whitespace: True
        skiprows: 49
        names:
          - "topo"
    cache:
      - type: compressed
        decomp: tgz
        argkey: urlpath
        regex_filter: 'aux3d.out'
```

```python
!rm -rf ~/.intake
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
%%time

download_zenodo_files(
    zenodo_doi=cat["MESH_AUX3D"].metadata["zenodo_doi"],
    target_directory=Path(cat["MESH_AUX3D"].urlpath).parent,
    force_download=False, 
    filter_pattern="*"
)
```

```python
cat["MESH_AUX3D"].cache[0].clear_all()
cat["MESH_AUX3D"].read()
```

```python
cat["MESH_NOD2D"].cache[0].clear_all()
cat["MESH_NOD2D"].read()
```

```python
cat["MESH_ELEM2D"].cache[0].clear_all()
cat["MESH_ELEM2D"].read()
```

```python
print(cat["FESOM2_sample"].read())
```
