import unittest
import json
import os
import tempfile

from xml_to_parquet.convert_xml_to_parquet import parse_file


class MyTest(unittest.TestCase):
    def test_parquet(self):

        realpath = os.path.dirname(os.path.realpath(__file__))

        input_file = os.path.join(realpath, "PurchaseOrder.xml")
        output_file = os.path.join(tempfile.gettempdir(), "PurchaseOrder.parquet")
        xsd_file = os.path.join(realpath, "PurchaseOrder.xsd")
        xpaths = None
        excludepaths = None

        parse_file(
            input_file, output_file, xsd_file, xpaths, excludepaths, None, None, None,
        )


if __name__ == "__main__":
    unittest.main()
