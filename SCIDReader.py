import argparse
import pandas as pd
import numpy as np
import struct
import sys

def SCIDReaderGen(scid_file):
    s_IntradayHeader = '=4sIIHHI36s'
    s_IntradayRecord = '=qffffIIII'
    header = scid.read(struct.calcsize(s_IntradayHeader))
    yield header

    while True:
        buf = scid.read(struct.calcsize(s_IntradayRecord))
        if not buf or len(buf) != struct.calcsize(s_IntradayRecord):
            break
        yield struct.unpack(s_IntradayRecord, buf)

def ConvertSCIDToDF(scid_file):
    reader = SCIDReaderGen(scid_file)

    # skip the header
    next(reader)

    df = pd.DataFrame(reader, columns=[
        'StartDateTime',
        'OpenPrice',
        'HighPrice',
        'LowPrice',
        'LastPrice',
        'NumTrades',
        'Volume',
        'BidVolume',
        'AskVolume'
    ])

    # convert to unix timestamp
    df.StartDateTime = (df.StartDateTime / 1000000 - (25569 * 86400)).astype(int)

    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', "-i", required=True, help="input scid file name")
    parser.add_argument('--output', '-o', default='-', help="output raw file")

    args = parser.parse_args()

    OUTPUT = args.output if args.output != '-' else sys.stdout

    with open(args.input, 'rb') as input_file:
        print(args.input + ' open successful and converting...', file=sys.stderr)
        df = ConvertSCIDToDF(input_file)
        print('Convertion done. Saving to file ' + args.output, file=sys.stderr)
        df.to_csv(OUTPUT, index=False)
        print('Saving done.', file=sys.stderr)

if __name__ == '__main__':
    main()
