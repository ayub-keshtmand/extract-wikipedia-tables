from src.utils.s3 import get_s3_client
from src.settings import (
    use_ssl,
    endpoint_url,
    aws_access_key_id,
    aws_secret_access_key,
)

def test_get_s3_client():
    s3_client = get_s3_client(
        use_ssl,
        endpoint_url,
        aws_access_key_id,
        aws_secret_access_key,
    )