#!/usr/bin/python
import csv
import random
import argparse
import logging
import string
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--records", type=int, required=False, default=1000)
parser.add_argument("--columns", type=int, required=False, default=400)
parser.add_argument("--datasize", type=int, required=False, default=36)
parser.add_argument("--filename", type=str, required=False, default="records.csv")

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

fieldnames = ["col{}".format(colnumber) for colnumber in range(0, args.columns)]
writer = csv.DictWriter(open(args.filename, "w"), fieldnames=fieldnames)
writer.writerow(dict(zip(fieldnames, fieldnames)))

choices_list = [
    "".join(random.choice(string.ascii_letters) for i in range(args.datasize))
    for choice in range(args.columns * 100)
]


for i in range(0, args.records):
    row = [
        ("col{}".format(colnumber), random.choice(choices_list))
        for colnumber in range(0, args.columns)
    ]
    writer.writerow(dict(row))
