# Project: Vacancy Parser

## Project Overview

This project consists of a set of Python scripts designed to scrape, store, and analyze job vacancy data from the Russian job board hh.ru. The primary goal is to track vacancy metrics, such as the number of responses over time, and identify trends.

The core functionalities are:
-   **Scraping**: Parsing vacancy search result pages from hh.ru.
-   **Data Storage**: Storing detailed vacancy data, including response history, into a MongoDB database.
-   **Analysis**: Calculating response rates and identifying top-performing vacancies.
-   **Reporting**: Generating Excel reports and plots based on the collected data.

The project uses Nuitka to compile the Python scripts into standalone executables.

## Key Files

-   `AutoParserMongoDB.py`: The main script for automated, continuous parsing. It scrapes vacancy data from a predefined hh.ru URL, stores it in a local MongoDB instance, and tracks the history of responses for each vacancy to calculate growth rates.
-   `Vacancy Parser.py`: A manual version of the parser. It prompts the user for a hh.ru URL and the number of pages to parse, then saves the extracted data into a timestamped Excel file.
-   `Plot_Hourly.py`: (Inferred) This script likely connects to the MongoDB database and uses a library like Matplotlib or Seaborn to generate the plots of hourly response rates found in the `/plots` directory.
-   `DBTest.py`: (Inferred) A utility script for testing the connection to the MongoDB database and verifying data integrity.
-   `.env`: A file to store environment variables, likely for configuration details such as database connection strings or API URLs, although the script currently defaults to a local MongoDB instance.

## Dependencies

Based on the source code, the following Python libraries are required:

-   `requests`
-   `beautifulsoup4`
-   `pandas`
-   `pymongo`
-   `pytz`
-   `matplotlib` / `seaborn` (Inferred for `Plot_Hourly.py`)

## Building and Running

### Running the Scripts

The scripts can be run directly using a Python interpreter.

1.  **Automated Parsing and Database Storage:**
    *   This script runs automatically and parses a hardcoded URL.
    *   Ensure a local MongoDB server is running.
    *   Execute the script:
        ```sh
        python AutoParserMongoDB.py
        ```

2.  **Manual Parsing to Excel:**
    *   This script is interactive.
    *   It will prompt you to enter a base URL from hh.ru and the number of pages to scrape.
    *   Execute the script:
        ```sh
        python "Vacancy Parser.py"
        ```

### Building Executables

The project uses Nuitka to create executables, as indicated by the `.spec` files and `build` directory.

```sh
# Example of how a build might be run (command is an assumption)
python -m nuitka --onefile "Vacancy Parser.py"
```

## Database

The `AutoParserMongoDB.py` script uses a MongoDB database to store data.

-   **Host**: `localhost`
-   **Port**: `27017`
-   **Database Name**: `hh_vacancies_db`
-   **Collection Name**: `vacancies`
