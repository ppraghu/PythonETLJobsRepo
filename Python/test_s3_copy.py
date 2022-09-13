import pytest
from s3_copy import get_input_filename, get_output_filename, copy_s3_input_to_output

def test_get_input_filename():
  assert get_input_filename() == 'source_data/input_data_file.csv'

def test_get_output_filename():
  output_filename = get_output_filename();
  assert output_filename.startswith('destination_data/output_data_file.') == True;
  assert output_filename.endswith('.csv') == True;

