
import argparse
import pandas as pd
import numpy as np
import struct
import sys
from InputOutput import WriteOutputByFormat, ReadInputByFormat

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', "-i", default='-', help="input tick file name")
    parser.add_argument('--output', '-o', default='-', help="output tick file")
    parser.add_argument('--sDateTime', required=True, type=int, help="Start DateTime")
    parser.add_argument('--eDateTime', type=int, default=0, help="End DateTime")
    parser.add_argument('--Oformat', "-f", default='csv', help="Output format")
    parser.add_argument('--Iformat', "-F", default='parquet', help="Input format")

    args = parser.parse_args()

    INPUT = args.input if args.input != '-' else sys.stdin
    OUTPUT = args.output if args.output != '-' else sys.stdout

    print('ExtractTicks Reading data from ' + args.input, file=sys.stderr)
    df = ReadInputByFormat(INPUT, args.Iformat)
    df = df[df.DateTime >= args.sDateTime]
    if args.eDateTime != 0:
        df = df[df.DateTime <= args.eDateTime]

    print('ExtractTicks Saving to ' + args.output, file=sys.stderr)
    WriteOutputByFormat(df, OUTPUT, args.Oformat, index=False)
    print('ExtractTicks Done.', file=sys.stderr)

if __name__ == '__main__':
    main()
