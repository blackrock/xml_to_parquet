"""
(c) 2021 BlackRock. All rights reserved.

Author: David Lee
"""
import io
import xml.etree.cElementTree as ET
import xmlschema
from collections import OrderedDict
import decimal
import json
import glob
from multiprocessing import Pool
import subprocess
import os
import gzip
import tarfile
import logging
import shutil
import sys
from zipfile import ZipFile
import pyarrow.parquet as arrow_parquet
import pyarrow.json as arrow_json
from datetime import datetime

# import time

from xmlschema.exceptions import XMLSchemaValueError
from xmlschema.compat import ordered_dict_class

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)


def json_decoder(obj):
    """
    :param obj: python data
    :return: converted type
    :raises:
    """
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(obj, set):
        return list(obj)
    raise TypeError(repr(obj) + " is not JSON serializable")


class NestedParqConverter(xmlschema.XMLSchemaConverter):
    """
    XML Schema based converter class for Parquet friendly json.
    """

    def __init__(self, namespaces=None, dict_class=None, list_class=None, **kwargs):
        """
        :param namespaces: map from namespace prefixes to URI.
        :param dict_class: dictionary class to use for decoded data. Default is `dict`.
        :param list_class: list class to use for decoded data. Default is `list`.
        """
        kwargs.update(attr_prefix="", text_key=None, cdata_prefix=None)
        super(NestedParqConverter, self).__init__(
            namespaces, dict_class or ordered_dict_class, list_class, **kwargs
        )

    def __setattr__(self, name, value):
        """
        :param name: attribute name.
        :param value: attribute value.
        :raises XMLSchemaValueError: Schema validation error for this converter
        """
        if name in ("text_key", "cdata_prefix") and value is not None:
            raise XMLSchemaValueError(
                "Wrong value %r for the attribute %r of a %r."
                % (value, name, type(self))
            )
        super(xmlschema.XMLSchemaConverter, self).__setattr__(name, value)

    @property
    def lossless(self):
        """
        :return: Returns back lossless property for this converter
        """
        return False

    def element_decode(self, data, xsd_element, level=0):
        """
        :param data: Decoded ElementData from an Element node.
        :param xsd_element: The `XsdElement` associated to decoded the data.
        :param level: 0 for root
        :return: A dictionary-based data structure containing the decoded data.
        """
        if data.attributes:
            self.attr_prefix = xsd_element.local_name + "@"
            result_dict = self.dict(
                [(k, v) for k, v in self.map_attributes(data.attributes)]
            )
        else:
            result_dict = self.dict()

        if xsd_element.type.is_simple() or xsd_element.type.has_simple_content():
            result_dict[xsd_element.local_name] = (
                data.text if data.text is not None and data.text != "" else None
            )

        if data.content:
            for name, value, xsd_child in self.map_content(data.content):
                if value:
                    if xsd_child.local_name:
                        name = xsd_child.local_name
                    else:
                        name = name[2 + len(xsd_child.namespace) :]

                    if xsd_child.is_single():
                        if hasattr(xsd_child, "type") and (
                            xsd_child.type.is_simple()
                            or xsd_child.type.has_simple_content()
                        ):
                            for k in value:
                                result_dict[k] = value[k]
                        else:
                            result_dict[name] = value
                    else:
                        if (
                            xsd_child.type.is_simple()
                            or xsd_child.type.has_simple_content()
                        ) and not xsd_child.attributes:
                            try:
                                result_dict[name].append(list(value.values())[0])
                            except KeyError:
                                result_dict[name] = self.list(value.values())
                            except AttributeError:
                                result_dict[name] = self.list(value.values())
                        else:
                            try:
                                result_dict[name].append(value)
                            except KeyError:
                                result_dict[name] = self.list([value])
                            except AttributeError:
                                result_dict[name] = self.list([value])
        if level == 0:
            return self.dict([(xsd_element.local_name, result_dict)])
        else:
            return result_dict


def open_file(zip, filename):
    """
    :param zip: whether to open a new file using gzip
    :param filename: name of new file
    :return: file handlers
    """
    if zip:
        return gzip.open(filename, "wb")
    else:
        return open(filename, "wb")


def parse_xml(
    xml_file,
    output_file,
    my_schema,
    xpaths_set,
    xparents_set,
    excludepaths_set,
    excludeparents_set,
    block_size,
    file_info_meta,
):
    """
    :param xml_file: xml file
    :param my_schema: xmlschema object
    :param xpaths_set: paths to include
    :param xparents_set: parent paths of includes
    :param excludepaths_set: paths to exclude
    :param excludeparents_set: parent paths of excludes
    :param processed: data found and processed previously
    :param block_size: memory needed to read in json data
    :param file_info_meta: capture file information metadata in parquet file
    :return: data found and processed
    """

    xparents = dict()
    excludeparents = dict()
    currentxpath = []
    json_data = ""

    if not xpaths_set:
        elem_active = True
    else:
        elem_active = False

    context = ET.iterparse(xml_file, events=("start", "end"))

    # Parse XML
    for event, elem in context:
        if event == "start":
            currentxpath.append(elem.tag.split("}", 1)[-1])

            currentxpath_key = tuple(currentxpath)

            xparents[currentxpath_key] = elem

            if currentxpath_key in xpaths_set:
                elem_active = True

            if elem_active and currentxpath_key in excludeparents_set:
                excludeparents[currentxpath_key] = elem

        if event == "end":

            currentxpath_key = tuple(currentxpath)
            parentxpath_key = tuple(currentxpath[:-1])

            if elem_active and currentxpath_key in excludepaths_set:
                excludeparents[parentxpath_key].remove(elem)

            if not elem_active and currentxpath_key not in xparents_set:
                xparents[parentxpath_key].remove(elem)

            if currentxpath_key in xpaths_set:
                elem_active = False

            del currentxpath[-1]

    try:
        my_dict = my_schema.to_dict(elem, process_namespaces=False, validation="skip")
        if file_info_meta:
            my_dict["file_info"] = file_info_meta
        my_json = json.dumps(my_dict, default=json_decoder)
    except Exception as ex:
        _logger.debug(ex)
        pass

    if not my_dict:
        return

    if block_size:
        arrow_data = arrow_json.read_json(
            io.BytesIO(bytes(my_json, "utf-8")),
            read_options=arrow_json.ReadOptions(block_size=block_size),
        )
    else:
        arrow_data = arrow_json.read_json(io.BytesIO(bytes(my_json, "utf-8")))

    _logger.debug("Saving to: " + output_file)

    arrow_parquet.write_table(arrow_data, output_file)


def parse_file(
    input_file,
    output_file,
    xsd_file,
    xpaths,
    excludepaths,
    delete_xml,
    block_size,
    file_info,
):
    """
    :param input_file: input file
    :param output_file: output file
    :param xsd_file: xsd file
    :param xpaths: whether to parse a specific xml path
    :param excludepaths: paths to exclude
    :param delete_xml: optional delete xml file after converting
    :param block_size: memory needed to read xml
    :param file_info: capture file information metadata in parquet file
    """

    _logger.debug("Generating schema from " + xsd_file)

    my_schema = xmlschema.XMLSchema(xsd_file, converter=NestedParqConverter)

    _logger.debug("Parsing " + input_file)

    xpaths_set = set()
    xparents_set = set()

    if xpaths:
        xpaths = xpaths.split(",")
        xpaths_list = [v.split("/")[1:] for v in xpaths]
        xpaths_set = {tuple(v) for v in xpaths_list}

        start = -1
        while True:
            x_set = {tuple(v[:start]) for v in xpaths_list if len(v[:start]) > 0}
            xparents_set.update(x_set)
            start = start - 1
            if not x_set:
                break

    excludepaths_set = set()
    excludeparents_set = set()

    if excludepaths:
        excludepaths = excludepaths.split(",")
        excludepaths_list = [v.split("/")[1:] for v in excludepaths]
        excludepaths_set = {tuple(v) for v in excludepaths_list}
        excludeparents_set = {tuple(v[:-1]) for v in excludepaths_list}

    if input_file.endswith(".tar.gz"):
        zip_file = tarfile.open(input_file, "r")

        zip_file_list = zip_file.getmembers()

        for member in zip_file_list:
            with zip_file.extractfile(member) as xml_file:
                if file_info:
                    file_info_meta = member.get_info()
                    file_info_meta["tarfile"] = os.path.basename(input_file)
                else:
                    file_info_meta = None

                parse_xml(
                    xml_file,
                    output_file + "." + member.name + ".parquet",
                    my_schema,
                    xpaths_set,
                    xparents_set,
                    excludepaths_set,
                    excludeparents_set,
                    block_size,
                    file_info_meta,
                )

    elif input_file.endswith(".zip"):
        zip_file = ZipFile(input_file, "r")

        if zip_file.testzip():
            logging.info("Zip File is Corrupt:" + input_file)
            return

        zip_file_list = zip_file.infolist()

        for i in range(len(zip_file_list)):
            with zip_file.open(zip_file_list[i].filename) as xml_file:
                if file_info:
                    file_info_meta = {
                        "filename": zip_file_list[i].filename,
                        "date_time": zip_file_list[i].date_time,
                        "compress_size": zip_file_list[i].compress_size,
                        "zipfile": os.path.basename(input_file),
                    }
                else:
                    file_info_meta = None

                parse_xml(
                    xml_file,
                    output_file + "." + zip_file_list[i].filename + ".parquet",
                    my_schema,
                    xpaths_set,
                    xparents_set,
                    excludepaths_set,
                    excludeparents_set,
                    block_size,
                    file_info_meta,
                )

    elif input_file.endswith(".gz"):
        with gzip.open(input_file) as xml_file:

            if file_info:
                file_info_meta = {
                    "filename": os.path.basename(input_file),
                    "modified": datetime.fromtimestamp(os.path.getmtime(input_file)),
                    "size": os.path.getsize(input_file),
                }
            else:
                file_info_meta = None

            parse_xml(
                xml_file,
                output_file + "." + input_file[:-3] + ".parquet",
                my_schema,
                xpaths_set,
                xparents_set,
                excludepaths_set,
                excludeparents_set,
                block_size,
                file_info_meta,
            )
    else:

        if file_info:
            file_info_meta = {
                "filename": os.path.basename(input_file),
                "modified": datetime.fromtimestamp(os.path.getmtime(input_file)),
                "size": os.path.getsize(input_file),
            }
        else:
            file_info_meta = None

        parse_xml(
            input_file,
            output_file + ".xml.parquet",
            my_schema,
            xpaths_set,
            xparents_set,
            excludepaths_set,
            excludeparents_set,
            block_size,
            file_info_meta,
        )

    if delete_xml:
        os.remove(input_file)

    _logger.debug("Completed " + input_file)


def convert_xml_to_parquet(
    xsd_file=None,
    target_path=None,
    xpaths=None,
    excludepaths=None,
    multi=1,
    verbose="DEBUG",
    log=None,
    delete_xml=None,
    block_size=None,
    file_info=None,
    xml_files=None,
):
    """
    :param xsd_file: xsd file name
    :param target_path: directory to save file
    :param xpaths: whether to parse a specific xml paths
    :param excludepaths: paths to exclude
    :param multi: how many files to convert concurrently
    :param verbose: stdout log messaging level
    :param log: optional log file
    :param delete_xml: optional delete xml file after converting
    :param block_size: memory needed to read one xml file
    :param file_info: capture file information metadata in parquet file
    :param xml_files: list of xml_files

    """

    formatter = logging.Formatter(
        "%(levelname)s - %(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(logging.getLevelName(verbose))
    _logger.addHandler(ch)

    if log:
        # create log file handler and set level to debug
        fh = logging.FileHandler(log)
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        _logger.addHandler(fh)

    _logger.info("Parsing XML Files..")

    if target_path and not os.path.exists(target_path):
        _logger.error("invalid target_path specified")
        sys.exit(1)

    # open target files
    file_list = list(
        set(
            [
                f
                for _files in [
                    glob.glob(xml_files[x]) for x in range(0, len(xml_files))
                ]
                for f in _files
            ]
        )
    )
    file_count = len(file_list)

    if multi > 1:
        parse_queue_pool = Pool(processes=multi)

    _logger.info("Processing " + str(file_count) + " files")

    if 1 < len(file_list) <= 1000:
        file_list.sort(key=os.path.getsize, reverse=True)
        _logger.info("Parsing files in the following order:")
        _logger.info(file_list)

    for filename in file_list:

        path, xml_file = os.path.split(os.path.realpath(filename))

        output_file = xml_file

        if output_file.endswith(".gz"):
            output_file = output_file[:-3]

        if output_file.endswith(".tar"):
            output_file = output_file[:-4]

        if output_file.endswith(".zip"):
            output_file = output_file[:-4]

        if output_file.endswith(".xml"):
            output_file = output_file[:-4]

        if target_path:
            output_file = os.path.join(target_path, output_file)
        else:
            output_file = os.path.join(path, output_file)

        if multi > 1:
            parse_queue_pool.apply_async(
                parse_file,
                args=(
                    filename,
                    output_file,
                    xsd_file,
                    xpaths,
                    excludepaths,
                    delete_xml,
                    block_size,
                    file_info,
                ),
                error_callback=_logger.info,
            )
        else:
            parse_file(
                filename,
                output_file,
                xsd_file,
                xpaths,
                excludepaths,
                delete_xml,
                block_size,
                file_info,
            )

    if multi > 1:
        parse_queue_pool.close()
        parse_queue_pool.join()
