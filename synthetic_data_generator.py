import os

import csv_util


def reset_case_studies(input_files_path: str, case_studies: list):
    for case_study in case_studies:
        input_path = f"{input_files_path}{case_study}"
        files = csv_util.find_csv_files(input_path, '.csv')
        for f in files:
            os.remove(os.path.join(input_path, f))


def generate_all_case_studies(input_files_path: str, case_studies: list, n_dts: int, n_snapshots_factor: int):
    for case_study in case_studies:
        # Get all template files
        input_path = f"{input_files_path}{case_study}"
        templates_path = f"{input_path}template/"
        templates = csv_util.find_csv_files(templates_path, '.csv')
        for template in templates:
            generate_dt(n_dts, n_snapshots_factor, templates_path, template, input_path)


def generate_dt(n_dts: int, n_snapshots_factor: int, template_file_path: str, template_filename: str,
                output_filepath: str):
    # Reader for template file
    dt_snps_file, dt_snps_reader = csv_util.get_reader(template_file_path + template_filename, ",")

    # Full csv file in a matrix
    dt_snps = list(dt_snps_reader)

    # Writer for output file
    writer_file, writer = csv_util.get_writer(
        f"{output_filepath}{n_dts}_{n_snapshots_factor * (len(dt_snps) - 1)}_{template_filename}.csv", ',')

    # File headers
    headers = dt_snps[0]
    writer.writerow(headers)

    # We assume the template file only includes one braccio
    for i in range(n_dts):
        previous_snp = 0
        twin_id = f"{dt_snps[1][headers.index('twinId')]}{i}"
        # Number of replications of the template execution
        for j in range(n_snapshots_factor):
            for k in range(1, len(dt_snps)):
                new_row = dt_snps[k][:]
                new_row[dt_snps[0].index('twinId')] = twin_id
                increment = int(dt_snps[k][dt_snps[0].index('timestamp')]) if (k - 1) == 0 else (
                        int(dt_snps[k][dt_snps[0].index('timestamp')]) - int(
                    dt_snps[k - 1][dt_snps[0].index('timestamp')]))
                new_row[dt_snps[0].index('timestamp')] = previous_snp = previous_snp + increment

                writer.writerow(new_row)

    writer_file.close()
    dt_snps_file.close()
