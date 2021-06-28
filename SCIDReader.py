import argparse
import pandas as pd
import numpy as np
import struct
import sys
from DataConvertionUtilities import ConvertSCIDToRaw

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', "-i", required=True, help="input scid file name")
    parser.add_argument('--output', '-o', default='-', help="output raw file")

    args = parser.parse_args()

    OUTPUT = args.output if args.output != '-' else sys.stdout

    with open(args.input, 'rb') as input_file:
        print('SCIDReader Reading and converting ' + args.input, file=sys.stderr)
        df = ConvertSCIDToRaw(input_file)
        print('SCIDReader Convertion done. Saving to file ' + args.output, file=sys.stderr)
        df.to_csv(OUTPUT, index=False)
        print('SCIDReader Saving done.', file=sys.stderr)

if __name__ == '__main__':
    main()
