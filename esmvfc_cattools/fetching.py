import os
from pathlib import Path
import pycurl
from urllib.parse import urlparse
import warnings

from .aux import file_has_checksum


def download_zenodo_files(
    zenodo_doi, target_directory=None, force_download=False, filter_pattern=None
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
    # get zenodo record ID from doi
    zenodo_record = zenodo_doi.split(".")[-1]
    logging.debug(f"will download record {zenodo_record}")

    # get full record from zenodo
    # see https://developers.zenodo.org/#quickstart-upload for pointers
    r = requests.get(f"https://zenodo.org/api/records/{zenodo_record}")
    logging.debug(f"got status code {r.status_code}")
    # should we debug-log the full json dump?

    # TODO: Check that we got the correct DOI

    # get list of source urls filtered for the file_pattern
    filtered_files = list(
        filter(lambda fn: fnmatch.fnmatch(fn["key"], filter_pattern), r.json()["files"])
    )
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
        # only download if file does not exist and not forced
        if not file.exists() or force_download:
            with open(file, "wb") as f:
                logging.debug(f"will download {url} to {file}")
                c = pycurl.Curl()
                c.setopt(c.URL, url)
                c.setopt(c.WRITEDATA, f)
                c.perform()
                c.close()
                logging.debug(f"download of {url} to {file} done")
            # check file if it was downloaded
            if not file_has_checksum(file_name=file, checksum=checksum):
                raise ValueError(f"Checksum for {file} does not match {checksum}")

    return all_target_files


def fetch_zenodo_data(catalog_entry, force_download=False):
    """DEPRECATED: Fetch data for `catalog_entry` from Zenodo.

    WARNING: This function will be removed from future versions of
    esmvfc_cattools. Use download_zenodo_files instead.

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
    warnings.warn(
        (
            "fetch_zenodo_data will be removed in future versions of esmvfc_cattools",
            " use download_zenodo_files instead.",
        ),
        PendingDeprecationWarning,
    )

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
