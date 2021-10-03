import pandas as pd
import argparse
import numpy as np
import random
import sys
import io
from DataConvertionUtilities import ConvertRaw2Tick
from InputOutput import WriteOutputByFormat, ReadInputByFormat

if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', "-i", default='-', help="Input file name")
    parser.add_argument('--output', "-o", default='-', help="Output file name")
    parser.add_argument('--Oformat', "-f", default='parquet', help="Output format")
    parser.add_argument('--Iformat', "-F", default='parquet', help="Input format")

    args = parser.parse_args()

    INPUT = args.input if args.input != '-' else sys.stdin
    OUTPUT = args.output if args.output != '-' else sys.stdout

    print('Raw2TickData Reading data from ' + args.input, file=sys.stderr)
    raw_df = ReadInputByFormat(INPUT, args.Iformat)
    print('Raw2TickData Converting to tick data', file=sys.stderr)
    df = ConvertRaw2Tick(raw_df)
    print('Raw2TickData Saving to ' + args.output, file=sys.stderr)
    WriteOutputByFormat(df, OUTPUT, args.Oformat)
    print('Raw2TickData Done.', file=sys.stderr)

