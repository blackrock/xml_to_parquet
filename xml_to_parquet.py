"""
(c) 2021 BlackRock. All rights reserved.

Author: David Lee
"""
import argparse

from xml_to_parquet_pkg.convert_xml_to_parquet import convert_xml_to_parquet

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="XML To JSON Parser")
    parser.add_argument("-x", "--xsd_file", required=True, help="xsd file name")
    parser.add_argument(
        "-t",
        "--target_path",
        help="target path. hdfs targets require hadoop client installation. Examples: /proj/test, hdfs:///proj/test, hdfs://halfarm/proj/test",
    )
    parser.add_argument(
        "-p",
        "--xpaths",
        help="xpaths to parse out. pass in comma separated string. /path/include1,/path/include2/*. add '*' to include all nested elements",
    )
    parser.add_argument(
        "-e",
        "--excludepaths",
        help="elements to exclude. pass in comma separated string. /path/exclude1,/path/exclude2",
    )
    parser.add_argument(
        "-m", "--multi", type=int, default=1, help="number of parsers. Default is 1."
    )
    parser.add_argument("-l", "--log", help="log file")
    parser.add_argument(
        "-v",
        "--verbose",
        default="DEBUG",
        help="verbose output level. INFO, DEBUG, etc.",
    )
    parser.add_argument(
        "-d",
        "--delete_xml",
        action="store_true",
        help="delete xml file after converting to json",
    )
    parser.add_argument(
        "-b", "--block_size", type=int, help="memory needed to read one xml file",
    )
    parser.add_argument(
        "-f",
        "--file_info",
        action="store_true",
        help="add file information metadata to parquet file",
    )
    parser.add_argument(
        "input_files", nargs=argparse.REMAINDER, help="files to convert"
    )

    args = parser.parse_args()

    convert_xml_to_parquet(
        args.xsd_file,
        args.target_path,
        args.xpaths,
        args.excludepaths,
        args.multi,
        args.verbose,
        args.log,
        args.delete_xml,
        args.block_size,
        args.file_info,
        args.input_files,
    )
