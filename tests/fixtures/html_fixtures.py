import pytest

@pytest.fixture
def html_file_path(tmpdir):
    """Create a temporary HTML file and provide its path."""
    HTML_STR = """
    <table>
        <tr>
            <th>A</th>
            <th colspan="1">B</th>
            <th rowspan="1">C</th>
        </tr>
        <tr>
            <td>a</td>
            <td>b</td>
            <td>c</td>
        </tr>
    </table>
    """
    path = tmpdir.join("tmp.html")
    with open(path, "w") as f:
        f.write(HTML_STR)
    return path
