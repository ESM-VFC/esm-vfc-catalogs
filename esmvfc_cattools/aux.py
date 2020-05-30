import hashlib


def file_has_checksum(file_name, checksum, blocksize=65536):
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
