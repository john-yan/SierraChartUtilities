
import argparse
import pandas as pd
import numpy as np
import struct
import sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', "-i", default='-', help="input tick file name")
    parser.add_argument('--output', '-o', default='-', help="output tick file")
    parser.add_argument('--sDateTime', required=True, type=int, help="Start DateTime")
    parser.add_argument('--eDateTime', type=int, default=0, help="End DateTime")

    args = parser.parse_args()

    INPUT = args.input if args.input != '-' else sys.stdin
    OUTPUT = args.output if args.output != '-' else sys.stdout

    print('ExtractTicks Reading CSV data from ' + args.input, file=sys.stderr)
    df = pd.read_csv(INPUT)
    df = df[df.DateTime >= args.sDateTime]
    if args.eDateTime != 0:
        df = df[df.DateTime <= args.eDateTime]

    print('ExtractTicks Saving to ' + args.output, file=sys.stderr)
    df.to_csv(OUTPUT, index=False)
    print('ExtractTicks Done.', file=sys.stderr)

if __name__ == '__main__':
    main()
