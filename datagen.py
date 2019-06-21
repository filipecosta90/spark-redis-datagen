#!/usr/bin/python
import csv
import random
import argparse
import logging
import string
import sys
import pyarrow.parquet as pq
import numpy as np
import pyarrow as pa
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--records", type=int, required=False, default=1000000)
parser.add_argument("--columns", type=int, required=False, default=400)
parser.add_argument("--datasize", type=int, required=False, default=36)
parser.add_argument("--filename", type=str, required=False, default="records")
parser.add_argument("--format", type=str, required=False, default="csv")

args = parser.parse_args()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

logger.debug(
    "Generating {} rows of {} columns each. Data size {} bytes.".format(
        args.records, args.columns, args.datasize
    )
)

filename_and_format = "{}_rec_{}_col_{}_dsize_{}.{}".format(args.filename,args.records, args.columns, args.datasize, args.format)
fieldnames = ["col{}".format(colnumber) for colnumber in range(0, args.columns)]

choices_list = [
    "".join(random.choice(string.ascii_letters) for i in range(args.datasize))
    for choice in range(args.columns * 100)
]


df = pd.DataFrame(columns=fieldnames, index=range(0, args.records))

for i in range(0, args.records):
    df.loc[i] = pd.Series( {
        "col{}".format(colnumber): random.choice(choices_list)
        for colnumber in range(args.columns)
    } )

if args.format == "csv":
    df.to_csv(filename_and_format, encoding='utf-8',index=False)


if args.format == "parquet":
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filename_and_format)
