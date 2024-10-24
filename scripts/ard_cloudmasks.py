import argparse
import logging
import os
import glob

from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(asctime)s: %(message)s')

def get_all_date_strings(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    return [(start+timedelta(days=x)).strftime("%Y%m%d") for x in range((end-start).days + 1)]

def get_filename_without_extensions(file_path):
    file = Path(file_path)

    while file.suffix:
        file = file.with_suffix('')

    return file.name

def get_ard_file_for_esa_product_name(ard_dir, esa_name):
    satellite = esa_name[0:3]
    date = esa_name[11:19]
    orbit = esa_name[34:37]
    tile = esa_name[38:44]

    pattern = f"{ard_dir}/**/{satellite}_{date}_*_{tile}*_ORB{orbit}_*_clouds.tif"
    ard_files = glob.glob(pattern, recursive=True)

    return ard_files

def main(start_date, end_date, input_dir, ard_dir, output_dir, symlink):
    logging.info(f"Finding cloud masks to rename between dates {start_date} and {end_date}")

    all_dates = get_all_date_strings(start_date, end_date)

    pattern = os.path.join(input_dir, "**", "*CLOUDMASK.tif")
    all_files = glob.glob(pattern, recursive=True)

    files_in_date_range = []
    for date in all_dates:
        matching_files = [file for file in all_files if date in file]
        files_in_date_range.extend(matching_files)

    logging.info(f"Found matching files {files_in_date_range}")

    esa_product_names = set()
    for file in files_in_date_range:
        product_name = get_filename_without_extensions(file)
        esa_product_names.add(product_name)

    logging.info(f"Found {len(esa_product_names)} ESA products")

    logging.info(f"Find matching ARD products")
    for product in esa_product_names:
        matching_ard = get_ard_file_for_esa_product_name(ard_dir, product)
        logging.info(f"Found matching {len(matching_ard)} ARD files")
        logging.info(matching_ard)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Create ARD compatible cloud mask files from the S2 cloudless outputs")
    parser.add_argument('-s', '--startdate', type=str, required=True, help='In YYYY-MM-dd format')
    parser.add_argument('-e', '--enddate', type=str, required=True, help='In YYYY-MM-dd format')
    parser.add_argument('-c', '--inputdir', type=str, required=True, help='Path to the cloud mask root dir')
    parser.add_argument('-a', '--arddir', type=str, required=True, help='Path to the ARD root dir')
    parser.add_argument('-o', '--outputdir', type=str, required=True, help='Path to the output directory for the renamed files')
    parser.add_argument('-l', '--symlink', type=bool, required=False, default=False, help='Create symlinks instead of hard copies')

    args = parser.parse_args()

    main(args.startdate, args.enddate, args.inputdir, args.arddir, args.outputdir, args.symlink)