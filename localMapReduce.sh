#!/bin/bash
cat Nintendo/input/nintendo_instagram.csv | python3 MapReducers/mapper.py | sort | python3 MapReducers/reducer.py Nintendo/input/nintendo_stock.csv > nintendo_output.json
cat goPro/input/gopro_instagram.csv | python3 MapReducers/mapper.py | sort | python3 MapReducers/reducer.py goPro/input/gopro_stock.csv > gopro_output_local.json
cat Tesla/input/tesla_instagram.csv | python3 MapReducers/mapper.py | sort | python3 MapReducers/reducer.py Tesla/input/tesla_stock.csv > tesla_output_local.json


