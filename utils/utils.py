import os

def get_raw_filename(filepath: str) -> str:
    """
    Extracts the base filename without its extension from a given file path.

    Parameters
    ----------
    filepath : str
        Full or relative path to the file.

    Returns
    -------
    str
        Filename without extension.
    """
    return os.path.splitext(os.path.basename(filepath))[0]
