# **XML To Parquet Converter**

This repository contains code for the XML to Parquet Converter.
This converter is written in Python and will convert one or more XML files into Parquet files

# Key Features

Converts XML to valid Parquet 

Requires only two files to get started. Your XML file and the XSD schema file for that XML file.

Multiprocessing enabled to parse XML files concurrently if the XML files are in the same format. Call with -m # option.

Uses Python's iterparse event based methods which enables parsing very large files with low memory requirements. This is very similar to Java's SAX parser

Files are processed in order with the largest files first to optimize overall parsing time

Option to write results to either Linux or HDFS folders

# Additional Notes

XML files with xs:union data types are not currently supported. A parquet column can only support a single data type.

For larger XML files the block_size parameter is required to allocate enough memory to capture your XML data.

# How to run?
```python
python xml_to_parquet.py
```

# Parameters
```python
usage: xml_to_parquet.py [-h] -x XSD_FILE [-t TARGET_PATH]
                         [-p XPATHS] [-e EXCLUDEPATHS] [-m MULTI] [-l LOG]
                         [-v VERBOSE] [-d] [-b BLOCK_SIZE]
                         ...

XML To Parquet Parser

positional arguments:
  xml_files             xml files to convert

optional arguments:
  -h, --help            show this help message and exit
  -x XSD_FILE, --xsd_file XSD_FILE
                        xsd file name
  -t TARGET_PATH, --target_path TARGET_PATH
                        target path. hdfs targets require hadoop client
                        installation. Examples: /proj/test, hdfs:///proj/test,
                        hdfs://halfarm/proj/test
  -p XPATHS, --xpaths XPATHS
                        xpaths to parse out. Pass in as a comma separated string.
                        /path/include1,/path/include2
  -e EXCLUDEPATHS, --excludepaths EXCLUDEPATHS
                        elements to exclude. Pass in as a comma separated string.
                        /path/exclude1,/path/exclude2
  -m MULTI, --multi MULTI
                        number of parsers. Default is 1.
  -l LOG, --log LOG     log file
  -v VERBOSE, --verbose VERBOSE
                        verbose output level. INFO, DEBUG, etc.
  -d, --delete          delete source file when completed
  -b, --block_size      allocate additional memory for large xml files in bytes
  -f, --file_info       capture file information metadata in parquet file


```

Sample XML
```xml
<?xml version="1.0"?>
<purchaseOrder orderDate="1999-10-20">
    <shipTo country="US">
        <name>Alice Smith</name>
        <street>123 Maple Street</street>
        <city>Mill Valley</city>
        <state>CA</state>
        <zip>90952</zip>
    </shipTo>
    <billTo country="US">
        <name>Robert Smith</name>
        <street>8 Oak Avenue</street>
        <city>Old Town</city>
        <state>PA</state>
        <zip>95819</zip>
    </billTo>
    <comment>Hurry, my lawn is going wild!</comment>
    <items>
        <item partNum="872-AA">
            <productName>Lawnmower</productName>
            <quantity>1</quantity>
            <USPrice>148.95</USPrice>
            <comment>Confirm this is electric</comment>
        </item>
        <item partNum="926-AA">
            <productName>Baby Monitor</productName>
            <quantity>1</quantity>
            <USPrice>39.98</USPrice>
            <shipDate>1999-05-21</shipDate>
        </item>
    </items>
</purchaseOrder>
```

# Convert a small XML file to a Parquet file
```python
python xml_to_parquet.py -x PurchaseOrder.xsd PurchaseOrder.xml

INFO - 2021-01-21 12:32:38 - Parsing XML Files..
INFO - 2021-01-21 12:32:38 - Processing 1 files
DEBUG - 2021-01-21 12:32:38 - Generating schema from PurchaseOrder.xsd
DEBUG - 2021-01-21 12:32:38 - Parsing PurchaseOrder.xml
DEBUG - 2021-01-21 12:32:38 - Saving to file PurchaseOrder.xml.parquet
DEBUG - 2021-01-21 12:32:38 - Completed PurchaseOrder.xml
```

JSON equivalent output
(zip code looks funny, but blame Microsoft which says zip is a decimal in the XSD file spec <xs:element name="zip" type="xs:decimal"/>)
```json
{"purchaseOrder":{"purchaseOrder@orderDate":"1999-10-20 00:00:00.000","shipTo":{"shipTo@country":"US","name":"Alice Smith","street":"123 Maple Street","city":"Mill Valley","state":"CA","zip":90952.0},"billTo":{"billTo@country":"US","name":"Robert Smith","street":"8 Oak Avenue","city":"Old Town","state":"PA","zip":95819.0},"comment":"Hurry, my lawn is going wild!","items":{"item":[{"item@partNum":"872-AA","productName":"Lawnmower","quantity":1,"USPrice":148.95,"comment":"Confirm this is electric","shipDate":null},{"item@partNum":"926-AA","productName":"Baby Monitor","quantity":1,"USPrice":39.98,"comment":null,"shipDate":"1999-05-21 00:00:00.000"}]}}}
```

# Convert an entire directory of XML files to Parquet
Parse 3 files concurrently and only extract /PurchaseOrder/items/item elements
```python
cp PurchaseOrder.xml 1.xml
cp 1.xml 2.xml
cp 1.xml 3.xml
cp 1.xml 4.xml

python xml_to_parquet.py -m 3 -p /purchaseOrder/items/item -x PurchaseOrder.xsd *.xml

INFO - 2021-01-21 12:38:00 - Parsing XML Files..
INFO - 2021-01-21 12:38:00 - Processing 5 files
INFO - 2021-01-21 12:38:00 - Parsing files in the following order:
INFO - 2021-01-21 12:38:00 - ['1.xml', '4.xml', 'PurchaseOrder.xml', '2.xml', '3.xml']
DEBUG - 2021-01-21 12:38:00 - Generating schema from PurchaseOrder.xsd
DEBUG - 2021-01-21 12:38:00 - Generating schema from PurchaseOrder.xsd
DEBUG - 2021-01-21 12:38:00 - Generating schema from PurchaseOrder.xsd
DEBUG - 2021-01-21 12:38:00 - Parsing 4.xml
DEBUG - 2021-01-21 12:38:00 - Parsing 1.xml
DEBUG - 2021-01-21 12:38:00 - Parsing PurchaseOrder.xml
DEBUG - 2021-01-21 12:38:00 - Saving to file 4.xml.parquet
DEBUG - 2021-01-21 12:38:00 - Saving to file PurchaseOrder.xml.parquet
DEBUG - 2021-01-21 12:38:00 - Saving to file 1.xml.parquet
DEBUG - 2021-01-21 12:38:00 - Completed 4.xml
DEBUG - 2021-01-21 12:38:00 - Generating schema from PurchaseOrder.xsd
DEBUG - 2021-01-21 12:38:00 - Completed PurchaseOrder.xml
DEBUG - 2021-01-21 12:38:00 - Completed 1.xml
DEBUG - 2021-01-21 12:38:00 - Generating schema from PurchaseOrder.xsd
DEBUG - 2021-01-21 12:38:00 - Parsing 3.xml
DEBUG - 2021-01-21 12:38:00 - Parsing 2.xml
DEBUG - 2021-01-21 12:38:00 - Saving to file 2.xml.parquet
DEBUG - 2021-01-21 12:38:00 - Saving to file 3.xml.parquet
DEBUG - 2021-01-21 12:38:00 - Completed 2.xml
DEBUG - 2021-01-21 12:38:00 - Completed 3.xml

```
JSON equivalent output for PurchaseOrder.parquet
```json
ls -l *.parquet
-rw-rw-r-- 1 leed users 3714 Jan 21 12:39 1.parquet
-rw-rw-r-- 1 leed users 3714 Jan 21 12:39 2.parquet
-rw-rw-r-- 1 leed users 3714 Jan 21 12:39 3.parquet
-rw-rw-r-- 1 leed users 3714 Jan 21 12:39 4.parquet
-rw-rw-r-- 1 leed users 3714 Jan 21 12:39 PurchaseOrder.parquet

{"purchaseOrder":{"purchaseOrder@orderDate":"1999-10-20 00:00:00.000","items":{"item":[{"item@partNum":"872-AA","productName":"Lawnmower","quantity":1,"USPrice":148.95,"comment":"Confirm this is electric","shipDate":null},{"item@partNum":"926-AA","productName":"Baby Monitor","quantity":1,"USPrice":39.98,"comment":null,"shipDate":"1999-05-21 00:00:00.000"}]}}}
```

# Exclude xpath elements
This removes xpaths from your result
```python
python xml_to_parquet.py -e /purchaseOrder/comment,/purchaseOrder/items -x PurchaseOrder.xsd PurchaseOrder.xml
```
JSON equivalent output
```json
{"purchaseOrder":{"purchaseOrder@orderDate":"1999-10-20 00:00:00.000","shipTo":{"shipTo@country":"US","name":"Alice Smith","street":"123 Maple Street","city":"Mill Valley","state":"CA","zip":90952.0},"billTo":{"billTo@country":"US","name":"Robert Smith","street":"8 Oak Avenue","city":"Old Town","state":"PA","zip":95819.0}}}
```
