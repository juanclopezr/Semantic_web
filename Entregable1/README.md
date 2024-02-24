# Semantic Web

**PDF Articles Crawler** - A python program to crawl semi-structured data from various articles publishers in the web.

## Dataset

The `data` folder contains the following resources.

- `data_4.parquet` A sample random gene expression matrix of small size

- `dict_split_4.json` A gene expression matrix of real experiments on mouse livers with 3600 genes

## Results

Data collected is ...

## How to run this program?

- Check if Python 3.9 is correctly installed.

```sh
python --version
```

- We recommend creating a virtual environment named `env` for the project.

On Linux/MacOS, run:

```sh
python3 -m venv env
```

On Windows, run:

```sh
python -m venv env
```

- Activate the virtual environment.

On Linux/MacOS, run:

```sh
source env/bin/activate
```

On Windows, run:

```sh
.\env\Scripts\activate
```

- Install required packages

```sh
pip install -r requirements.txt
```

- Run the program

```sh
python .\index.py --option request_bulk
```

The program receives the following required parameters:

- `--option` This should be the command to execute. Available options are `request_bulk` to send API calls to collect data to complement the existing dataset.