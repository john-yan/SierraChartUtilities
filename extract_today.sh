#!/bin/bash

# activate python virtualenv
source env/bin/activate

PYTHON=$(which python3)

ES_SYMBOL=ESU1-CME
NQ_SYMBOL=NQU1-CME

FROM=$(date --date='yesterday 17:59:59' +"%s")
TO=$(date --date='today 17:00:00' +"%s")
$PYTHON SCIDReader.py -i ~/netdrive/john/SierraChart/Data/ESU1-CME.scid |\
  $PYTHON Raw2TickData.py |\
  $PYTHON ExtractTicks.py --sDateTime=$FROM --eDateTime=$TO -o today.tick

