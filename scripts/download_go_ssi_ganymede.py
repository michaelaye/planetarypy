#!/usr/bin/env python3
import argparse
from pathlib import Path

from tqdm.auto import tqdm

from planetarypy.instruments.go_ssi import EDR, get_edr_index


def download_data(save_folder):
    """Download Galileo SSI EDR data for Ganymede observations."""
    # Ensure the save folder exists
    save_path = Path(save_folder)
    save_path.mkdir(parents=True, exist_ok=True)

    print(f"Downloading Ganymede data to: {save_path.absolute()}")

    ganymedes = get_edr_index().query("TARGET_ID == 'GANYMEDE'")
    print(f"Found {len(ganymedes)} Ganymede observations")

    for image_id in tqdm(ganymedes.IMAGE_ID, desc="Downloading images"):
        edr = EDR(image_id, save_folder)
        edr.download_all()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Download Galileo SSI EDR data for Ganymede observations",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "save_folder",
        type=str,
        help="Directory to save the downloaded data (will be created if it doesn't exist)",
    )

    args = parser.parse_args()

    try:
        download_data(args.save_folder)
        print("Download completed successfully!")
    except Exception as e:
        print(f"Error during download: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
