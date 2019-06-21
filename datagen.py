#!/usr/bin/python
import argparse
import logging
import random
import string

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

parser = argparse.ArgumentParser()
parser.add_argument("--records", type=int, required=False, default=1000000)
parser.add_argument("--columns", type=int, required=False, default=400)
parser.add_argument("--datasize", type=int, required=False, default=36)
parser.add_argument("--filename", type=str, required=False, default="records")
parser.add_argument("--format", type=str, required=False, default="csv")

args = parser.parse_args()

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create console handler and set level to debug
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# add formatter to ch
handler.setFormatter(formatter)

# add handler to logger
logger.addHandler(handler)

logger.debug(
    "Generating {} rows of {} columns each. Data size {} bytes.".format(
        args.records, args.columns, args.datasize
    )
)

filename_and_format = "{}_rec_{}_col_{}_dsize_{}.{}".format(args.filename, args.records, args.columns, args.datasize,
                                                            args.format)
fieldnames = ["col{}".format(colnumber) for colnumber in range(0, args.columns)]

logger.info("creating random values")

choices_list = [
    "".join(random.choice(string.ascii_letters) for i in range(args.datasize))
    for choice in range(args.columns * 1000)
]

df = pd.DataFrame(columns=fieldnames, index=range(0, args.records))

logger.info("creating records")

for i in tqdm(range(0, args.records)):
    df.loc[i] = pd.Series({
        "col{}".format(colnumber): random.choice(choices_list)
        for colnumber in range(args.columns)
    })

if args.format == "csv":
    logger.info("started saving to csv format")
    df.to_csv(filename_and_format, encoding='utf-8', index=False)
    logger.info("ended saving to parquet format")

if args.format == "parquet":
    logger.info("started saving to parquet format")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filename_and_format)
    logger.info("ended saving to parquet format")
