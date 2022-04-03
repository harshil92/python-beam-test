# python-beam-test
This pipeline reads transaction data and applies the following filters and transforms:
1. Find all transactions that have a `transaction_amount` greater than `20`
2. Exclude all transactions made before the year `2010`
3. Sum the total by `date`
4. Compresses the csv file into gzip and stores it in the `output` folder.

### Install
```
pip install -r requirements.txt
```

### Usage
#### Run locally using DirectRunner
```
python main.py \
--input path/to/file \
--output path/to/output/file
```
- Both options contain default value so they are optional. 


#### Unit test
Run the following command from root directory.
```
python -m unittest -v
```
