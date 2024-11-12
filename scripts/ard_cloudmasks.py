import argparse
import logging
import os
import glob
import shutil
import re
import csv

from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(asctime)s: %(message)s')

def get_all_date_strings(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    return [(start+timedelta(days=x)).strftime("%Y%m%d") for x in range((end-start).days + 1)]

def get_cloudmask_files(start_date, end_date, input_dir):
    all_dates = get_all_date_strings(start_date, end_date)

    pattern = os.path.join(input_dir, "**", "*CLOUDMASK.tif")
    all_files = glob.glob(pattern, recursive=True)

    files_in_date_range = []
    for date in all_dates:
        matching_files = [file for file in all_files if date in file]
        files_in_date_range.extend(matching_files)

    return files_in_date_range

def get_filename_without_extensions(file_path):
    file = Path(file_path)

    while file.suffix:
        file = file.with_suffix('')

    return file.name

def get_esa_product_names(cloudmask_files):
    esa_product_names = set()
    for file in cloudmask_files:
        product_name = get_filename_without_extensions(file)
        esa_product_names.add(product_name)

    return esa_product_names

def get_ard_files(esa_product_name, ard_dir):
    satellite = esa_product_name[0:3]
    date = esa_product_name[11:19]
    orbit = esa_product_name[34:37]
    tile = esa_product_name[38:44]

    pattern = f"{ard_dir}/**/{satellite}_{date}_*_{tile}*_ORB{orbit}_*_clouds.tif"
    ard_files = glob.glob(pattern, recursive=True)

    return ard_files

def get_all_esa_splits(esa_product, esa_products):
    product_basename = esa_product[0:44]
    esa_splits = list(filter(lambda x: x.startswith(product_basename), esa_products))
    
    return esa_splits

def ard_names_have_processing_times(ard_files):
    pattern = "S2[A-B]_[0-9]{8}_[0-9a-zA-Z]{10,}_T[0-9A-Z]{5}_ORB[0-9]{3}_[0-9]{14}"

    files_with_processing_times = 0
    for file in ard_files:
        if re.search(pattern, file):
            files_with_processing_times += 1

    if files_with_processing_times != 0 and files_with_processing_times != len(ard_files):
        raise Exception(f"Found a mix of new and old ARD names: {ard_files}")
    
    return files_with_processing_times == len(ard_files)

def get_processing_time_from_ard_name(ard_file):
    # example filepath: /path/to/file/S2B_20240619_latn527lone0008_T30UYD_ORB137_20240619120339_utm30n_osgb_clouds.tif

    filename = os.path.basename(ard_file)
    processing_time = filename.split("_")[5]

    return processing_time

def get_matching_split(product, esa_product_names, matching_ard_files):
    esa_splits = get_all_esa_splits(product, esa_product_names)

    if len(esa_splits) > len(matching_ard_files):
        raise Exception(f"Can't match split granules - there are more ESA splits than ARD splits")

    esa_splits_sorted = sorted(esa_splits, key=lambda y: y[44:-1]) # sort by processing time
    
    ard_products_sorted = []
    if ard_names_have_processing_times(matching_ard_files):
        ard_products_sorted = sorted(matching_ard_files, key=lambda y: get_processing_time_from_ard_name(y))
    else:
        ard_products_sorted = sorted(matching_ard_files, reverse=True)

    matching_split = ""
    for i, split in enumerate(esa_splits_sorted):
        if esa_splits_sorted[i] == product:
            matching_split = ard_products_sorted[i]
            break

    return matching_split

def match_esa_names_to_ard_names(esa_product_names, ard_dir):
    esa_ard_mappings = {}

    for esa_name in esa_product_names:
        logging.info(f"Find matching ARD products for {esa_name}...")
        matching_ard_files = get_ard_files(esa_name, ard_dir)
        logging.info(f"Found {len(matching_ard_files)} matching ARD files")
        
        if len(matching_ard_files) == 1:
            esa_ard_mappings[esa_name] = os.path.basename(matching_ard_files[0])
        elif len(matching_ard_files) > 1:
            matching_split = get_matching_split(esa_name, esa_product_names, matching_ard_files)

            logging.info(f"Found matching split {matching_split} for {esa_name}")
            esa_ard_mappings[esa_name] = os.path.basename(matching_split)
        else:
            raise Exception(f"Couldn't find matching ARD file for {esa_name}")

    return esa_ard_mappings

def get_file_mappings(esa_ard_mappings, cloudmask_files, output_dir):
    file_mappings = {}

    for esa_product in esa_ard_mappings:
        esa_filepath =  [filepath for filepath in cloudmask_files if esa_product in os.path.basename(filepath)][0]

        ard_filename = esa_ard_mappings[esa_product]
        date_dirs = os.path.join(ard_filename[4:8], ard_filename[8:10], ard_filename[10:12])
        ard_filepath = os.path.join(output_dir, date_dirs, ard_filename)

        file_mappings[esa_filepath] = ard_filepath

    return file_mappings

def create_output_files(file_mappings, symlink):
    for esa_filepath, ard_filepath in file_mappings.items():
        ard_folder = os.path.dirname(ard_filepath)
        os.makedirs(ard_folder, exist_ok=True)

        if symlink:
            logging.info(f"Creating symlink from {ard_filepath} to {esa_filepath}")
            if os.path.exists(ard_filepath):
                os.remove(ard_filepath)
            os.symlink(esa_filepath, ard_filepath)
        else:
            logging.info(f"Copying file from {ard_filepath} to {esa_filepath}")
            shutil.copy(esa_filepath, ard_filepath)

def get_product_version_mappings(esa_products):
    '''
    get mapping like
    {
        "S2A_MSIL1C_20200731T113331_R080_T30VVM": {
            "0209": [
                "S2A_MSIL1C_20200731T113331_N0209_R080_T30VVM_20200731T114957",
                "S2A_MSIL1C_20200731T113331_N0209_R080_T30VVM_20200731T115955"
            ],
            "0500": [
                "S2A_MSIL1C_20200731T113331_N0500_R080_T30VVM_20230327T001417",
                "S2A_MSIL1C_20200731T113331_N0500_R080_T30VVM_20230414T031220"
            ]
        },
        ...
    }
    '''

    product_mappings = {}
    for product_name in esa_products:
        swath_name = f"{product_name[0:26]}_{product_name[33:44]}"
        version = product_name[28:32]

        if swath_name in product_mappings:
            product_mapping = product_mappings[swath_name]

            if version in product_mapping:
                product_mapping[version].append(product_name)
            else:
                product_mapping[version] = [product_name]
        else:
            product_mappings[swath_name] = {
                version: [product_name]
            }

    return product_mappings

def get_latest_versions(esa_product_names):
    latest_versions = set()
    
    product_mappings = get_product_version_mappings(esa_product_names)

    for swath_name in product_mappings:
        mapping = product_mappings[swath_name]

        products = []
        if len(mapping) == 1:
            # only one version available
            products = next(iter(mapping.values()))
        else:
            # multiple versions available, use the latest version
            latest_version = sorted(mapping.keys(), reverse=True)[0]
            products = mapping[latest_version]

        latest_versions.update(products)
            
    return latest_versions

def generate_report(file_mappings, report_file):
    with open(report_file, "w", newline="") as file:
        writer = csv.writer(file, quoting=csv.QUOTE_ALL)
        writer.writerow(["Input cloudmask file", "Output cloudmask file", "Timestamp"])

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for old_filepath, new_filepath in file_mappings.items():
            writer.writerow([old_filepath, new_filepath, timestamp])

def main(start_date, end_date, input_dir, ard_dir, output_dir, symlink, report_file):
    logging.info(f"Finding cloud mask files between dates {start_date} and {end_date}")

    cloudmask_files = get_cloudmask_files(start_date, end_date, input_dir)

    logging.info(f"Found {len(cloudmask_files)} cloud mask files")

    esa_product_names = get_esa_product_names(cloudmask_files)
    esa_product_names = get_latest_versions(esa_product_names)

    logging.info(f"Found {len(esa_product_names)} ESA products")

    esa_ard_mappings = match_esa_names_to_ard_names(esa_product_names, ard_dir)
    file_mappings = get_file_mappings(esa_ard_mappings, cloudmask_files, output_dir)

    logging.info(f"Creating {len(file_mappings)} output files...")

    create_output_files(file_mappings, symlink)

    if report_file:
        logging.info(f"Generating CSV report at {report_file}")
        generate_report(file_mappings, report_file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Create ARD compatible cloud mask files from the S2 cloudless outputs")
    parser.add_argument('-s', '--startdate', type=str, required=True, help='In YYYY-MM-dd format')
    parser.add_argument('-e', '--enddate', type=str, required=True, help='In YYYY-MM-dd format')
    parser.add_argument('-c', '--inputdir', type=str, required=True, help='Path to the cloud mask root dir')
    parser.add_argument('-a', '--arddir', type=str, required=True, help='Path to the ARD root dir')
    parser.add_argument('-o', '--outputdir', type=str, required=True, help='Path to the output directory for the renamed files')
    parser.add_argument('-l', '--symlink', type=bool, required=False, default=False, help='Create symlinks instead of hard copies')
    parser.add_argument('-r', '--reportfile', type=str, required=False, help='Path to report file output')

    args = parser.parse_args()

    main(args.startdate, args.enddate, args.inputdir, args.arddir, args.outputdir, args.symlink, args.reportfile)