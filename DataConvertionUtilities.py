
import pandas as pd
import numpy as np
import struct
import random

def SCIDReaderGen(scid):
    s_IntradayHeader = '=4sIIHHI36s'
    s_IntradayRecord = '=qffffIIII'
    header = scid.read(struct.calcsize(s_IntradayHeader))
    yield header

    while True:
        buf = scid.read(struct.calcsize(s_IntradayRecord))
        if not buf or len(buf) != struct.calcsize(s_IntradayRecord):
            break
        yield struct.unpack(s_IntradayRecord, buf)

def ConvertSCIDToDF(scid_file, datetime_is_seconds=True):
    reader = SCIDReaderGen(scid_file)

    # skip the header
    next(reader)

    raw = pd.DataFrame(reader, columns=[
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
    raw.StartDateTime = raw.StartDateTime - (25569 * 86400 * 1000000)

    if datetime_is_seconds:
        raw.StartDateTime = (raw.StartDateTime / 1000000).astype('int')

    return raw

def SCDepthReaderGen(depth):
    s_MarketDepthFileHeader = '=4I48s'
    s_MarketDepthFileRecord = '=qbbhfII'
    header = depth.read(struct.calcsize(s_MarketDepthFileHeader))

    yield struct.unpack(s_MarketDepthFileHeader, header)

    while True:
        record = depth.read(struct.calcsize(s_MarketDepthFileRecord))
        if not record or len(record) != struct.calcsize(s_MarketDepthFileRecord):
            break
        yield struct.unpack(s_MarketDepthFileRecord, record)

def ConvertSCDepthToDF(depth_file):
    reader = SCDepthReaderGen(depth_file)

    # skip the header
    next(reader)

    raw = pd.DataFrame(reader, columns=[
        'DateTime',
        'Command',
        'Flags',
        'NumOrders',
        'Price',
        'Quantity',
        'Reserved'
    ])

    # convert to unix timestamp
    raw.DateTime = raw.DateTime - (25569 * 86400) * 1000_000
    raw.drop(columns=['Reserved'], inplace=True)

    return raw

def ConvertRaw2Tick(raw_df):
    raw_df['AtBidOrAsk'] = (raw_df.Volume == raw_df.AskVolume).astype(np.int32) + 1
    check = (raw_df.Volume != raw_df.AskVolume) & (raw_df.Volume != raw_df.BidVolume)
    if check.any() == True:
        print('Correcting these errors')
        print(raw_df[check])
        for i in raw_df[check].index:
            if raw_df.iloc[i].HighPrice != raw_df.iloc[i].LowPrice:
                at_ask = abs(raw_df.iloc[i].LastPrice - raw_df.iloc[i].HighPrice) > abs(raw_df.iloc[i].LastPrice - raw_df.iloc[i].LowPrice)
                raw_df.at[i, 'AtBidOrAsk'] = int(at_ask) + 1
            else:
                raw_df.at[i, 'AtBidOrAsk'] = random.choice([1, 2])

        print('After corrections')
        print(raw_df[check])

    return pd.DataFrame({
        'DateTime': raw_df.StartDateTime,
        'Price': raw_df.LastPrice,
        'Volume': raw_df.Volume,
        'AtBidOrAsk': raw_df.AtBidOrAsk
    })

def ConvertTick2OHLC(ticks, period=30*60):
    ticks = pd.DataFrame(ticks.reset_index(drop=True))
    periods = (ticks.DateTime / period).astype(np.int32)
    diff = periods.diff()
    diff[0] = 1
    start_index = np.array(ticks[diff != 0].index)
    end_index = np.roll(start_index, -1)
    end_index[-1] = len(ticks)
    new_index = np.arange(0, len(start_index), 1)
    new_index = np.repeat(new_index, end_index - start_index)
    ticks['new_index'] = new_index
    group = ticks.groupby(['new_index'])
    result = pd.DataFrame({
        'StartDateTime': group.DateTime.first(),
        'EndDateTime': group.DateTime.last(),
        'Open': group.Price.first(),
        'High': group.Price.max(),
        'Low': group.Price.min(),
        'Close': group.Price.last(),
        'Volume': group.Volume.sum()
    })

    return result
