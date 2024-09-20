# PPE - Inventory to Purchase Matching Tool

Welcome to the **PPE Inventory to Purchase Matching Tool**! This project is designed to streamline the process of matching assets from Freshservice with their corresponding purchase orders from Airtable. By integrating data from multiple sources, processing and standardizing it, and providing an interactive interface using Streamlit, this tool helps in efficiently managing and reconciling inventory assets with their purchases.

## Table of Contents

- [Project Overview](#project-overview)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Project Structure](#project-structure)
- [Setup and Installation](#setup-and-installation)
  - [Prerequisites](#prerequisites)
  - [Installation Steps](#installation-steps)
- [Usage](#usage)
  - [Running the Data Pipeline and Streamlit App](#running-the-data-pipeline-and-streamlit-app)
  - [Using the Streamlit App](#using-the-streamlit-app)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)
- [Contact Information](#contact-information)

## Project Overview

The **PPE Inventory to Purchase Matching Tool** automates the retrieval and processing of laptop asset data from Freshservice and purchase data from Airtable. It standardizes the data using OpenAI's GPT API, performs necessary mappings, and provides an interactive interface to match laptop assets to their corresponding purchases. This tool aims to simplify the reconciliation process between assets and purchases, reduce manual errors, and enhance data consistency across systems.

## Features

- **Data Retrieval**: Automatically fetches laptop asset data from Freshservice and purchase order data from Airtable using their APIs.
- **Data Processing**: Cleans, standardizes, and merges data from different sources, focusing on laptops.
- **Interactive Matching**: Provides a Streamlit web app to interactively match laptop assets to their corresponding purchases.
- **Data Standardization**: Utilizes OpenAI's GPT API to standardize vendor names, product names, and asset types.
- **Automated Mappings**: Creates mappings between assets and purchases based on various criteria.
- **Persistent Assignments**: Saves assignment data for future reference and continued work.
- **Testing Suite**: Includes a comprehensive set of unit tests to ensure code reliability.
- **Modular Design**: Organized codebase with clear separation of concerns for ease of maintenance.

## Technologies Used

- **Python 3.8+**
- **Pandas**: Data manipulation and analysis.
- **Requests**: HTTP requests to interact with APIs.
- **Python Dotenv**: Loading environment variables from a `.env` file.
- **PyAirtable**: Airtable API client.
- **OpenAI API**: For data standardization using GPT.
- **Streamlit**: Building the interactive web application.
- **RapidFuzz**: Fuzzy string matching for better matching accuracy.
- **Pytest**: Testing framework.
- **Poetry**: Dependency management and packaging.

## Project Structure

```
.
├── data/                     # Data files (input/output)
├── notebooks/                # Jupyter notebooks for EDA and prototyping
├── src/                      # Source code
│   ├── __init__.py
│   ├── data_retrieval_freshservice.py
│   ├── data_retrieval_airtable.py
│   ├── data_processing_freshservice.py
│   ├── processing_headcount.py
│   ├── data_standardization.py
│   ├── laptop_matching.py
│   └── main.py
├── tests/                    # Test suites
│   ├── __init__.py
│   ├── test_data_retrieval_freshservice.py
│   ├── test_data_retrieval_airtable.py
│   ├── test_data_processing_freshservice.py
│   ├── test_processing_headcount.py
│   ├── test_data_standardization.py
│   └── test_laptop_matching.py
├── .env                      # Environment variables (not included in repo)
├── .gitignore
├── pyproject.toml            # Poetry configuration file
├── poetry.lock               # Poetry lock file
├── README.md
└── LICENSE
```

## Setup and Installation

### Prerequisites

- **Python 3.8 or higher**
- **Virtual Environment**: Recommended to avoid dependency conflicts.
- **API Keys**: You will need API keys for:
  - Freshservice
  - Airtable
  - OpenAI

### Installation Steps

1. **Clone the Repository**

   ```bash
   git clone https://github.com/ahmetybesiroglu/ppe.git
   cd ppe
   ```

2. **Install Poetry**

   Poetry is used for dependency management.

   ```bash
   pip install poetry
   ```

3. **Install Dependencies**

   ```bash
   poetry install
   ```

   This command will create a virtual environment and install all the required packages.

4. **Set Up Environment Variables**

   Create a `.env` file in the project root directory with the following content:

   ```env
   # Freshservice API
   FRESHSERVICE_DOMAIN=your_freshservice_domain
   FRESHSERVICE_API_KEY=your_freshservice_api_key

   # Airtable API
   AIRTABLE_API_KEY=your_airtable_api_key
   SANDBOX_BASE_ID=your_sandbox_base_id
   HEADCOUNTTRACKER_BASE_ID=your_headcounttracker_base_id
   NETSUITE_TABLE_ID=your_netsuite_table_id
   HEADCOUNT_TABLE_ID=your_headcount_table_id
   FILEWAVE_TABLE_ID=your_filewave_table_id

   # OpenAI API
   OPENAI_API_KEY=your_openai_api_key
   ```

   **Note**: Keep this file secure and do not commit it to version control.

5. **Set Up Pre-Commit Hooks (Optional)**

   If you wish to use pre-commit hooks for code formatting and linting:

   ```bash
   poetry run pre-commit install
   ```

## Usage

### Running the Data Pipeline and Streamlit App

To run the entire data processing pipeline and launch the Streamlit app, execute:

```bash
poetry run python main.py
```

This command will:

- Retrieve and process laptop asset data from Freshservice and purchase order data from Airtable.
- Standardize and merge the data, focusing on laptops.
- Launch the Streamlit web application for interactive matching.

### Using the Streamlit App

Once the app is running, it will be accessible at `http://localhost:8501` by default.

**Features of the App:**

- **Asset Details**: View detailed information about each laptop asset.
- **Purchase Matching**: Interactively match laptop assets to corresponding purchase orders.
- **Suggested Matches**: Provides exact and fuzzy matches based on vendor, product name, and purchase date.
- **Assignment Summary**: See an overview of all assignments made.
- **Data Persistence**: Assignments are saved automatically and can be reloaded.

**Navigating the App:**

- **Next/Previous Asset**: Use the buttons to navigate between assets.
- **Assign Purchase**: Select a purchase from the suggested matches to assign to an asset.
- **Unassign Purchase**: Remove an assignment if necessary.

**Exiting the App:**

- To stop the Streamlit app, press `Ctrl+C` in the terminal where it's running.

## Testing

This project includes a comprehensive suite of unit tests using `pytest`. Tests are located in the `tests/` directory.

To run the tests, execute:

```bash
poetry run pytest
```

To generate a coverage report:

```bash
poetry run pytest --cov=src tests/
```

## Contributing

Contributions are welcome! Here's how you can help:

1. **Fork the Repository**

   Click the "Fork" button at the top right of the repository page.

2. **Clone Your Fork**

   ```bash
   git clone https://github.com/yourusername/ppe.git
   cd ppe
   ```

3. **Create a New Branch**

   ```bash
   git checkout -b feature/your-feature-name
   ```

4. **Make Changes and Commit**

   - Follow the existing code style and structure.
   - Write or update tests as necessary.
   - Commit your changes with descriptive commit messages.

5. **Push to Your Fork**

   ```bash
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request**

   Open a pull request on the original repository, describing your changes in detail.

**Please ensure that your contributions adhere to the project's coding standards and pass all tests.**

## License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and distribute this software in accordance with the license terms.

## Contact Information

For any inquiries or issues, please open an issue on the [GitHub repository](https://github.com/ahmetybesiroglu/ppe) or contact the project maintainer:

- **Name**: Ahmet Besiroglu
- **Email**: [ahmetybesiroglu@gmail.com](mailto:ahmetybesiroglu@gmail.com)
- **LinkedIn**: [Ahmet Besiroglu](https://www.linkedin.com/in/ahmetbesiroglu/)

---

I hope this README more accurately reflects your project. Let me know if there's anything else you'd like me to adjust or include!
