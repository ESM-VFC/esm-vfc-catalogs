import os
from pathlib import Path
import pycurl
from urllib.parse import urlparse


def fetch_zenodo_data(catalog_entry, force_download=False):
    """Fetch data for `catalog_entry` from Zenodo.

    Parameters
    ----------
    catalog_entry : obj
        An intake catalog entry.  We'll download all files from the
        `catalog_entry.metadata["data_urls"]` list and put it in
        `f'{os.environ["ESM_VFC_DATA_DIR"]}/{catalog_entry.cat.name}/'`.

    force_download : bool
        If `True`, download will be forced even if the target file exists.
        Defaults to `False`.

    """
    # set output directory and ensure it exists
    output_dir = Path(os.environ["ESM_VFC_DATA_DIR"]) / catalog_entry.cat.name
    output_dir.mkdir(parents=True, exist_ok=True)

    # for all urls, get data
    for url in catalog_entry.metadata["data_urls"]:

        file_name = Path(urlparse(url).path).name
        output_file = output_dir / file_name

        if output_file.exists() and not force_download:
            print(f"No need to download {output_file}")
        else:
            print(f"downloading {output_file} ... ", end="")
            with open(output_file, "wb") as f:
                c = pycurl.Curl()
                c.setopt(c.URL, url)
                c.setopt(c.WRITEDATA, f)
                c.perform()
                c.close()
            print("... done")
