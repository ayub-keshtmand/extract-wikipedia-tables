import pytest
from src.pipeline.extract import get_html_tables_from_url

def test_extract_table_from_temp_file(html_file_path):
    result = get_html_tables_from_url(f"file://{html_file_path}")
    assert len(result) == 1
    assert list(result[0].columns) == ["A", "B", "C"]
    assert result[0].iloc[0].to_list() == ["a", "b", "c"]
