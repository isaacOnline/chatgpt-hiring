# ChatGPT Hiring
This repository contains code for testing ChatGPT using the HybridHiring dataset

## Usage
* The directory `hybridhiring` can be downloaded from [here](https://www.microsoft.com/en-us/download/details.aspx?id=105296) 
and should be placed in the `input_data` directory.
* The conda environment I used for the project can be recreated using `environment.yml`.
* Currently, searches for each URL in each common crawl mentioned by [De-Arteaga et al.](https://arxiv.org/abs/1901.09451)
manually, which is very slow. Searching the indexes is very slow, so I think we'll likely want to search the CC indexes 
using something like [this](https://github.com/commoncrawl/cc-pyspark) 