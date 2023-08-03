# cli-utility

Provides a collection of useful tools.

# local install

Depending on your local python setup,
replace `python with python3` and `pip with pip3`

```
git clone https://github.com/pattarika/cli-utility
cd cli-utility
pre-commit install
python -m venv .venv
pip install --upgrade pip
pip install -r requirements.txt
```

# cheatsheet command

```
python bin/ak-utility.py -h

# miscelleanouse tools about delivery config
python bin/ak-utility.py -a $account delivery-config -h
python bin/ak-utility.py -a $account delivery-config behavior -h
python bin/ak-utility.py -a $account delivery-config custom-behavior -h

# miscelleanouse tools about security config
python bin/ak-utility.py -a $account security -h
python bin/ak-utility.py -a $account security hostname -h


```
