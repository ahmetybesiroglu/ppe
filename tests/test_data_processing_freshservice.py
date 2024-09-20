# tests/test_data_processing_freshservice.py

import pytest
import pandas as pd
from src import data_processing_freshservice as dpf

def test_flatten_type_fields():
    df = pd.DataFrame({
        'id': [1],
        'type_fields': ['{"key1": "value1", "key2": "value2"}']
    })
    flattened_df = dpf.flatten_type_fields(df, column_name='type_fields')
    assert 'key1' in flattened_df.columns
    assert 'key2' in flattened_df.columns
    assert flattened_df.loc[0, 'key1'] == 'value1'
    assert flattened_df.loc[0, 'key2'] == 'value2'

def test_clean_column_names():
    df = pd.DataFrame(columns=['column_1', 'column_2_123'])
    cleaned_df = dpf.clean_column_names(df)
    assert 'column_1' in cleaned_df.columns
    assert 'column_2' in cleaned_df.columns

def test_map_departments():
    assets_df = pd.DataFrame({'department_id': [1, 2]})
    departments_df = pd.DataFrame({'department_id': [1], 'department_name': ['Dept A']})
    mapped_df = dpf.map_departments(assets_df, departments_df)
    assert 'department_name' in mapped_df.columns
    assert mapped_df.loc[0, 'department_name'] == 'Dept A'
    assert pd.isnull(mapped_df.loc[1, 'department_name'])
