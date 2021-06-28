
import pandas as pd
import argparse
import numpy as np
import random
import sys
from DataConvertionUtilities import ConvertRaw2Tick

if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', "-i", default='-', help="Input file name")
    parser.add_argument('--output', "-o", default='-', help="Output file name")

    args = parser.parse_args()

    INPUT = args.input if args.input != '-' else sys.stdin
    OUTPUT = args.output if args.output != '-' else sys.stdout

    print('Raw2TickData Reading CSV data from ' + args.input, file=sys.stderr)
    raw_df = pd.read_csv(INPUT)
    print('Raw2TickData Converting to tick data', file=sys.stderr)
    df = ConvertRaw2Tick(raw_df)

    print('Raw2TickData Saving to ' + args.output, file=sys.stderr)
    df.to_csv(OUTPUT, index=False)
    print('Raw2TickData Done.', file=sys.stderr)

