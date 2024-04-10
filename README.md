# LinkedIn Career Path Navigator
> Data Collection Lab (094290) - Technion

## Running the Code
The project consists of two parts: The notebook and the search engine.
To run the notebook, you need access to the course's Databricks environment. Just upload the notebook and run it.
To run the search engine, install the libraries listed in `requirements.txt` and run like any other python script (We were using Python 3.11).

## Job Search Engine
The job search engine is using the [`retriv`](https://github.com/AmenRa/retriv/tree/main) library. The library excepts json lines (.jsonl) as an input. 
The `to_jsonl.py` script generated this file. To run this file you will need to place the scraped jobs in a folder called "dice" in the same folder as the script. Contact us for the scraped jobs.