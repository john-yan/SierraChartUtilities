
import pandas as pd
import numpy as np
import struct

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

def ConvertSCIDToRaw(scid_file):
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
    raw.StartDateTime = (raw.StartDateTime / 1000000 - (25569 * 86400)).astype(int)

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
    ticks['period'] = (ticks.DateTime / period).astype(np.int32)
    ticks['last_period'] = np.roll(ticks.period, 1)
    ticks.at[0, 'last_period'] = 0
    ticks['last_period'] = ticks.last_period.astype(np.int32)
    start_index = np.array(ticks[ticks.period != ticks.last_period].index)
    end_index = np.roll(start_index, -1)
    end_index[-1] = len(ticks)
    new_index = np.arange(0, len(start_index), 1)
    new_index = np.repeat(new_index, end_index - start_index)
    ticks['new_index'] = new_index
    result = pd.DataFrame(np.zeros((len(start_index), 7)), columns=['StartDateTime', 'EndDateTime', 'Open', 'High', 'Low', 'Close', 'Volume'])
    group = ticks.groupby(['new_index'])

    result['StartDateTime'] = group.DateTime.first()
    result['EndDateTime'] = group.DateTime.last()
    result['Open'] = group.Price.first()
    result['High'] = group.Price.max()
    result['Low'] = group.Price.min()
    result['Close'] = group.Price.last()
    result['Volume'] = group.Volume.sum()

    return result

