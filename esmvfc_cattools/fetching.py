import fnmatch
import logging
import os
from pathlib import Path
import pycurl
import re
import requests
from urllib.parse import urlparse
import warnings
from tqdm.auto import tqdm

from .aux import file_has_checksum


def _parse_urlpath(urlpath):
    """Parse urlpath and find the file part.

    We assume that urlpaths either are just a path or that they are composed
    by url chaining (see
    <https://filesystem-spec.readthedocs.io/en/latest/features.html#url-chaining>)
    and look something like "simplecache::zip://data.csv::file://archive.zip"
    with the file:// block at the end.
    """
    try:
        return re.search(r"file://(.*)", urlpath).group(1)
    except AttributeError as e:
        return urlpath


def download_zenodo_files_for_entry(cat_entry, force_download=False):
    """Download files for entry from Zenodo.

    Parameters
    ----------
    cat_entry : intake catalaog entry
        Catalog entry to download for. Needs `.metadata.zenodo_doi` and `.args.urlpath`.
        The DOI will be used to find out from where to download the data. The `urlpath`
        will be used to find out which files to download and where to store the
        downloaded files.
    force_download : bool
        Download even if files already exist?  Defaults to False.

    Returns
    -------
    List of file paths that have been downloaded.
    """
    # if urlpath is a string, just parse and pass the `Path(urlpath).name`
    # as filter pattern
    # otherwise, iterate over urlpaths
    if isinstance(cat_entry.urlpath, str):
        urlpath = _parse_urlpath(cat_entry.urlpath)
        target_files = download_zenodo_files(
            zenodo_doi=cat_entry.metadata["zenodo_doi"],
            target_directory=str(Path(urlpath).parent),
            filter_pattern=str(Path(urlpath).name),
            force_download=force_download,
        )
    else:  # not checking if iterable
        for urlpath in cat_entry.urlpath:
            urlpath = _parse_urlpath(urlpath)
            target_files = download_zenodo_files(
                zenodo_doi=cat_entry.metadata["zenodo_doi"],
                target_directory=str(Path(urlpath).parent),
                filter_pattern=str(Path(urlpath).name),
                force_download=force_download,
            )
    return target_files


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
        filter(
            lambda fn: (
                (filter_pattern is None) or (fnmatch.fnmatch(fn["key"], filter_pattern))
            ),
            r.json()["files"],
        )
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
            print(f"will download {url} to {file}")
            with tqdm(total=100, unit="B", unit_scale=True) as t_progress:

                def _tqdm_progress_func(download_t, download_d, upload_t, upload_d):
                    # if not set yet, set total download size
                    if t_progress.total != download_t:
                        t_progress.reset(download_t)
                    # update to downloaded volume and refresh
                    t_progress.n = download_d
                    t_progress.refresh()

                with open(file, "wb") as f:
                    c = pycurl.Curl()
                    c.setopt(c.URL, url)
                    c.setopt(c.WRITEDATA, f)
                    c.setopt(c.NOPROGRESS, False)
                    c.setopt(c.XFERINFOFUNCTION, _tqdm_progress_func)
                    c.perform()
                    c.close()

            logging.debug(f"download of {url} to {file} done")
            # check file if it was downloaded
            if file_has_checksum(file_name=file, checksum=checksum):
                logging.debug(f"checksum {checksum} for {file} matches.")
            else:
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
