# AssetTracker: Comprehensive Inventory Management System

AssetTracker is an advanced inventory management system designed to streamline the tracking and management of an organization's physical assets. This project integrates data from multiple sources to provide a unified view of assets, their purchase details, and their association with employees. By incorporating accounting principles such as Property, Plant, and Equipment (PPE) depreciation schedules, AssetTracker offers a holistic solution for asset management, financial tracking, and compliance.

## Table of Contents

- [Features](#features)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Configuration](#configuration)
  - [Environment Variables](#environment-variables)
  - [Airtable Base Schema](#airtable-base-schema)
- [Usage](#usage)
- [Workflow Overview](#workflow-overview)
- [Scripts Explanation](#scripts-explanation)
- [Dependencies](#dependencies)
- [Contributing](#contributing)
- [License](#license)
- [Contact Information](#contact-information)

---

## Features

- **Automated Data Retrieval**: Fetch asset, purchase, and employee data from Freshservice and Airtable APIs.
- **Data Processing and Cleaning**: Process raw data, handle missing values, and standardize formats for consistency.
- **Accounting Integration**: Incorporate PPE depreciation schedules for accurate financial reporting and compliance.
- **Asset-Purchase Matching**: Automatically match assets with corresponding invoices and purchase records.
- **Employee Assignment**: Link assets to employees based on usage and assignment data.
- **Manual Matching Interface**: Streamlit application for manual matching of assets to purchases and employees.
- **Inventory Management**: Maintain an up-to-date inventory of assets, their statuses, and locations.
- **Airtable Synchronization**: Push processed data back to Airtable, creating a centralized database with linked relationships.
- **Robust Error Handling**: Comprehensive logging and error handling to ensure data integrity.
- **Modular Design**: Each component of the system is modular, facilitating maintenance and scalability.

---

## Project Structure

```plaintext
.
├── main.py
├── README.md
├── pyproject.toml
├── poetry.lock
├── src
│   ├── __init__.py
│   ├── data_retrieval_freshservice.py
│   ├── data_retrieval_airtable.py
│   ├── data_processing_freshservice.py
│   ├── data_processing_headcount.py
│   ├── data_standardization.py
│   ├── laptop_matching.py
│   ├── matching_streamlit_app.py
│   ├── headcount_matching.py
│   ├── push_to_asset_types.py
│   ├── push_to_assets.py
│   ├── push_to_departments.py
│   ├── push_to_employees.py
│   ├── push_to_products.py
│   ├── push_to_purchases.py
│   ├── push_vendors.py
│   └── link_tables.py
└── tests
    ├── __init__.py
    └── test_*.py
```

---

## Installation

### Prerequisites

- Python 3.8 or higher
- [Poetry](https://python-poetry.org/docs/#installation) for dependency management
- Access to Freshservice and Airtable APIs
- OpenAI API key for data standardization
- Optional: Streamlit for manual matching interface

### Steps

1. **Clone the Repository**

   ```bash
   git clone https://github.com/ahmetybesiroglu/ppe.git
   cd ppe
   ```

2. **Install Dependencies**

   Use Poetry to install all required packages.

   ```bash
   poetry install
   ```

3. **Set Up Environment Variables**

   Create a `.env` file in the root directory and add the required environment variables as described in the [Environment Variables](#environment-variables) section.

---

## Configuration

### Environment Variables

Ensure all API keys and environment variables are correctly set in the `.env` file. The system relies on these configurations to access external services and APIs.

```env
# Freshservice API
FRESHSERVICE_DOMAIN=your_freshservice_domain
FRESHSERVICE_API_KEY=your_freshservice_api_key

# Airtable API
AIRTABLE_API_KEY=your_airtable_api_key
SANDBOX_BASE_ID=your_sandbox_base_id
HEADCOUNTTRACKER_BASE_ID=your_headcount_tracker_base_id

# OpenAI API
OPENAI_API_KEY=your_openai_api_key

# Airtable Table IDs
NETSUITE_TABLE_ID=tbl975mir3khzqJYA
FILEWAVE_TABLE_ID=tblM8Ktd0Iep92dHM
ASSETS_TABLE_ID=tblhQ3aXuwILrUqAk
VENDORS_TABLE_ID=tbligXSSHUEmCnjHz
PRODUCTS_TABLE_ID=tblhTuJLW5Ba5lTIS
ASSET_TYPES_TABLE_ID=tblu8jcet4SVks2No
DEPARTMENTS_TABLE_ID=tblQB3AAijsw8TlLf
EMPLOYEES_TABLE_ID=tblLbYKezmDx6AWky
PURCHASES_TABLE_ID=your_purchases_table_id
HEADCOUNT_TABLE_ID=your_headcount_table_id
```

Replace `your_*` placeholders with your actual API keys and IDs.

### Airtable Base Schema

The system uses an Airtable base with multiple tables to store and manage data. Below is a detailed description of each table and its fields.

#### 1. **Netsuite Table (`tbl975mir3khzqJYA`)**

This table stores purchase records from Netsuite.

- **Fields:**
  - `date` (Date): Purchase date.
  - `parent class` (Single select): Classification of the asset.
  - `done` (Checkbox): Status indicator.
  - `update` (Multiple select): Update status.
  - `asset class` (Single select): Asset class type.
  - `reference` (Text): Reference number.
  - `vendor` (Text): Vendor name.
  - `description` (Text): Purchase description.
  - `cost` (Number): Purchase cost.
  - `Item` (Text): Item name.
  - `count` (Number): Quantity purchased.
  - `note` (Text): Additional notes.
  - `useful life` (Text): Asset's useful life.
  - `serial` (Link to `filewave`): Serial numbers linked from the Filewave table.
  - `employee` (Text): Employee associated with the purchase.

#### 2. **Filewave Table (`tblM8Ktd0Iep92dHM`)**

This table contains device information from Filewave.

- **Fields:**
  - `Name` (Text): Device name.
  - `Platform` (Text): Operating system.
  - `Version` (Text): OS version.
  - `Last Logged Username` (Text): Username of the last person who logged in.
  - `Last connect` (Text): Last connection date.
  - `Netsuite` (Link to `netsuite`): Linked purchase records.

#### 3. **Assets Table (`tblhQ3aXuwILrUqAk`)**

This table tracks physical assets.

- **Fields:**
  - `asset` (Formula): Concatenation of `display_id` and `product`.
  - `display_id` (Text): Display ID of the asset.
  - `asset_id` (Text): Unique asset ID.
  - `name` (Text): Asset name.
  - `vendor` (Link to `vendors`): Vendor who supplied the asset.
  - `product` (Link to `products`): Product details.
  - `asset_type` (Lookup from `product`): Asset type derived from the linked product.
  - `assigned_to` (Link to `employees`): Employee assigned to the asset.
  - `cost` (Currency): Cost of the asset.
  - `description` (Long text): Asset description.
  - `serial_number` (Text): Serial number.
  - `acquisition_date` (Date): Date of acquisition.
  - `created_at` (Date): Asset creation date.
  - `assigned_on` (Date): Date when the asset was assigned.
  - `asset_state` (Single select): Current state (e.g., In Use, In Stock).
  - `purchase_id` (Link to `purchases`): Associated purchase record.

#### 4. **Vendors Table (`tbligXSSHUEmCnjHz`)**

This table contains vendor information.

- **Fields:**
  - `name` (Text): Vendor name.
  - `vendor_id` (Text): Unique vendor ID.
  - `contact_name` (Text): Contact person at the vendor.
  - `email` (Email): Contact email.
  - `mobile` (Phone number): Contact phone number.
  - `address` (Long text): Vendor address.
  - `assets` (Link to `assets`): Assets supplied by the vendor.
  - `product_supplied` (Link to `products`): Products supplied.

#### 5. **Products Table (`tblhTuJLW5Ba5lTIS`)**

This table lists products.

- **Fields:**
  - `name` (Text): Product name.
  - `product_id` (Text): Unique product ID.
  - `manufacturer` (Text): Manufacturer name.
  - `vendor` (Link to `vendors`): Vendor supplying the product.
  - `asset_type` (Link to `asset_types`): Asset type category.
  - `description` (Long text): Product description.
  - `assets` (Link to `assets`): Associated assets.

#### 6. **Asset Types Table (`tblu8jcet4SVks2No`)**

This table categorizes asset types.

- **Fields:**
  - `name` (Text): Asset type name.
  - `asset_type_id` (Text): Unique asset type ID.
  - `parent_asset_type` (Link to `asset_types`): Parent asset type.
  - `note` (Long text): Additional notes.
  - `products` (Link to `products`): Products under this asset type.
  - `assets` (Lookup from `products`): Assets under this asset type.

#### 7. **Departments Table (`tblQB3AAijsw8TlLf`)**

This table records department information.

- **Fields:**
  - `name` (Text): Department name.
  - `department_id` (Text): Unique department ID.
  - `employees` (Link to `employees`): Employees in the department.
  - `assets` (Lookup from `employees`): Assets used by the department.

#### 8. **Employees Table (`tblLbYKezmDx6AWky`)**

This table maintains employee records.

- **Fields:**
  - `full_name` (Formula): Concatenation of `first_name` and `last_name`.
  - `employee_id` (Text): Unique employee ID.
  - `first_name` (Text): Employee's first name.
  - `last_name` (Text): Employee's last name.
  - `masterworks_email` (Email): Work email address.
  - `status` (Single select): Employment status.
  - `employee_type` (Single select): Type of employment.
  - `title` (Text): Job title.
  - `position_start_date` (Date): Start date of the position.
  - `termination_date` (Date): Termination date, if applicable.
  - `department` (Link to `departments`): Department affiliation.
  - `assets` (Link to `assets`): Assets assigned to the employee.

---

## Usage

### Running the Inventory Management Pipeline

Activate the Poetry shell and run the main script:

```bash
poetry shell
python main.py
```

### Pipeline Steps

1. **Automated Data Retrieval and Processing**

   - The system starts by fetching data from Freshservice and Airtable.
   - Data is processed and cleaned to ensure consistency and readiness for further steps.

2. **Data Standardization**

   - Utilizes OpenAI GPT-4 to standardize vendor names, product names, and asset types.
   - Standardization mappings are cached to optimize API usage.

3. **Asset-Purchase Matching**

   - Automatically matches assets with purchase invoices based on vendor and product information.
   - Incorporates accounting PPE depreciation schedules for financial accuracy.

4. **Manual Matching (Optional)**

   - Launch the Streamlit app for manual matching:

     ```bash
     streamlit run src/matching_streamlit_app.py
     ```

   - Follow the on-screen instructions to manually match assets with purchases and employees.

5. **Employee Assignment**

   - Assets are linked to employees using headcount data and usage logs.
   - Fuzzy matching is employed to associate assets with the correct employees.

6. **Data Push to Airtable**

   - Processed data is pushed back to Airtable.
   - Tables are linked appropriately, creating a centralized and relational database.

---

## Workflow Overview

1. **Data Retrieval**

   - `data_retrieval_freshservice.py`: Fetches asset and purchase data from Freshservice.
   - `data_retrieval_airtable.py`: Retrieves purchase invoices and employee data from Airtable.

2. **Data Processing**

   - `data_processing_freshservice.py`: Processes and cleans data from Freshservice, including flattening nested structures.
   - `data_processing_headcount.py`: Processes employee headcount data for matching.

3. **Data Standardization**

   - `data_standardization.py`: Standardizes vendor names, product names, and asset types across datasets using OpenAI GPT-4.

4. **Asset-Purchase Matching**

   - `laptop_matching.py`: Automatically matches assets with purchase invoices.
   - `matching_streamlit_app.py`: Provides a manual interface for matching assets to purchases and employees.

5. **Employee Assignment**

   - `headcount_matching.py`: Matches assets with employees based on usage data and logs.

6. **Data Synchronization**

   - `push_to_*.py`: Scripts to push processed data to Airtable and maintain an up-to-date inventory:
     - `push_to_asset_types.py`
     - `push_to_assets.py`
     - `push_to_departments.py`
     - `push_to_employees.py`
     - `push_to_products.py`
     - `push_to_purchases.py`
     - `push_vendors.py`
   - `link_tables.py`: Establishes relationships between assets, purchases, employees, and other entities in Airtable.

---

## Scripts Explanation

- **`main.py`**: Orchestrates the entire inventory management pipeline by invoking the necessary scripts in sequence.
- **`data_retrieval_freshservice.py`**: Connects to the Freshservice API to retrieve asset and purchase data, including PPE depreciation schedules.
- **`data_retrieval_airtable.py`**: Fetches data from Airtable tables such as NetSuite invoices, headcount tracker, and FileWave.
- **`data_processing_freshservice.py`**: Processes and cleans data from Freshservice, preparing it for standardization.
- **`data_processing_headcount.py`**: Processes employee data to filter active employees and prepare for asset assignment.
- **`data_standardization.py`**: Standardizes critical fields across datasets to ensure consistency.
- **`laptop_matching.py`**: Automates the matching of assets to purchase invoices and incorporates accounting depreciation schedules.
- **`matching_streamlit_app.py`**: A user-friendly interface for manual matching of assets to purchases and employees.
- **`headcount_matching.py`**: Links assets to employees using fuzzy matching algorithms.
- **`push_to_*.py`**: Series of scripts to push processed data to Airtable and maintain an up-to-date inventory:
  - `push_to_asset_types.py`
  - `push_to_assets.py`
  - `push_to_departments.py`
  - `push_to_employees.py`
  - `push_to_products.py`
  - `push_to_purchases.py`
  - `push_vendors.py`
- **`link_tables.py`**: Establishes relationships between different entities in Airtable, such as linking assets to employees and purchases.

---

## Dependencies

- **Python Packages**

  - `requests`
  - `pandas`
  - `python-dotenv`
  - `pyairtable`
  - `openai`
  - `rapidfuzz`
  - `streamlit`
  - `ast`
  - `logging`

- **External Services**

  - **Freshservice API**: For asset and purchase data retrieval, including depreciation schedules.
  - **Airtable API**: For data retrieval and synchronization.
  - **OpenAI API**: For data standardization using GPT-4.

---

## Contributing

Contributions are welcome! Please follow these steps:

1. **Fork the Repository**

2. **Create a Feature Branch**

   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Commit Your Changes**

   ```bash
   git commit -m "Your detailed description of the changes."
   ```

4. **Push to Your Fork**

   ```bash
   git push origin feature/your-feature-name
   ```

5. **Create a Pull Request**

---

## License

This project is licensed under the [MIT License](LICENSE).

---

## Contact Information

For any questions or inquiries, please contact:

- **Name**: Ahmet Besiroglu
- **Email**: ahmetybesiroglu@gmail.com
- **LinkedIn**: [Ahmet Besiroglu](https://www.linkedin.com/in/ahmetybesiroglu/)
- **GitHub**: [Ahmet Besiroglu](https://github.com/ahmetybesiroglu)

---

Thank you for exploring AssetTracker! This project showcases expertise in Python programming, API integration, data processing, and the development of comprehensive inventory management systems. AssetTracker not only automates the tedious aspects of asset management but also integrates accounting principles to provide valuable insights into asset depreciation and financial planning.
