from pathlib import Path

import requests


def download_file(url, local_path, overwrite=False):
    """Download a file from a URL to a local path."""
    local_path = Path(local_path)

    # Check if file already exists and we're not overwriting
    if local_path.exists() and not overwrite:
        print(f"File already exists: {local_path}")
        return local_path

    # Create parent directories if they don't exist
    local_path.parent.mkdir(parents=True, exist_ok=True)

    # Download the file
    print(f"Downloading {url} to {local_path}")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    return local_path


# Define version mapping
base_url = "https://naif.jpl.nasa.gov/pub/naif/generic_kernels/pck/"
pck_urls = {
    "v11": f"{base_url}pck00011.tpc",
    "v10": f"{base_url}pck00010.tpc",
    "v09": f"{base_url}a_old_versions/pck00009.tpc",
    "v08": f"{base_url}a_old_versions/pck00008.tpc",
    "v07": f"{base_url}a_old_versions/pck00007.tpc",
    "v06": f"{base_url}a_old_versions/pck00006.tpc",
    "v05": f"{base_url}a_old_versions/pck00005.tpc",
    "v03": f"{base_url}a_old_versions/pck00003.tpc",
}


def get_kernel(version="v11", data_dir=Path(".") / "pck_kernels"):
    """Get the path to a PCK kernel file, downloading it if necessary."""
    if version not in pck_urls:
        raise ValueError(f"Unknown version: {version}")

    data_dir.mkdir(parents=True, exist_ok=True)
    url = pck_urls[version]
    filename = f"pck_{version}.tpc"
    local_path = data_dir / filename

    return download_file(url, local_path)
