import pytest
from unittest.mock import patch
from canine.localization.file_handlers import (
    get_file_handler,
    hash_set,
    FileType,
    StringLiteral,
    HandleGSURL,
    HandleAWSURL,
    HandleRODISKURL,
    HandleDRSURI,
    HandleGCSSignedURL,
    HandleGDCHTTPURL,
    HandleOtherURL,
)


# ---------------------------------------------------------------------------
# hash_set
# ---------------------------------------------------------------------------

class TestHashSet:

    def test_deterministic(self):
        s = {"b", "a", "c"}
        assert hash_set(s) == hash_set(s)

    def test_order_independent(self):
        assert hash_set({"a", "b", "c"}) == hash_set({"c", "a", "b"})

    def test_different_sets_differ(self):
        assert hash_set({"a", "b"}) != hash_set({"a", "c"})

    def test_single_element(self):
        h = hash_set({"only"})
        assert isinstance(h, str) and len(h) == 32  # md5 hex

    def test_requires_set_type(self):
        with pytest.raises(AssertionError):
            hash_set(["a", "b"])


# ---------------------------------------------------------------------------
# FileType base class hash
# ---------------------------------------------------------------------------

class TestFileTypeBaseHash:

    def test_same_path_same_hash(self):
        f1 = StringLiteral("gs://bucket/file.txt")
        f2 = StringLiteral("gs://bucket/file.txt")
        assert f1.hash == f2.hash

    def test_different_paths_different_hashes(self):
        f1 = StringLiteral("gs://bucket/a.txt")
        f2 = StringLiteral("gs://bucket/b.txt")
        assert f1.hash != f2.hash

    def test_hash_is_cached(self):
        f = StringLiteral("some_path")
        h1 = f.hash
        h2 = f.hash
        assert h1 is h2  # same object (cached)


# ---------------------------------------------------------------------------
# HandleRODISKURL._get_hash
# ---------------------------------------------------------------------------

class TestHandleRODISKURLHash:

    def test_crc32c_disk_returns_hash_suffix(self):
        f = HandleRODISKURL("rodisk://canine-crc32c-abc123def456/myfile.txt")
        assert f._get_hash() == "abc123def456"

    def test_non_crc32c_disk_returns_full_url(self):
        url = "rodisk://canine-someotherdisk/myfile.txt"
        f = HandleRODISKURL(url)
        assert f._get_hash() == url

    def test_non_canine_disk_returns_full_url(self):
        url = "rodisk://user-custom-disk/data.bam"
        f = HandleRODISKURL(url)
        assert f._get_hash() == url

    def test_invalid_url_no_path_raises(self):
        with pytest.raises(ValueError):
            HandleRODISKURL("rodisk://diskonly").  _get_hash()

    def test_invalid_url_completely_malformed_raises(self):
        with pytest.raises(ValueError):
            HandleRODISKURL("not-a-rodisk-url")._get_hash()


# ---------------------------------------------------------------------------
# get_file_handler — URL routing dispatch
# ---------------------------------------------------------------------------

class TestGetFileHandler:
    """All tests mock os.path.exists to return False so local-file branch is skipped."""

    @pytest.fixture(autouse=True)
    def no_local_files(self):
        with patch("os.path.exists", return_value=False):
            yield

    def test_filetype_object_returned_as_is(self):
        ft = StringLiteral("already_a_filetype")
        assert get_file_handler(ft) is ft

    def test_gs_url(self):
        h = get_file_handler("gs://bucket/path/to/file.txt")
        assert isinstance(h, HandleGSURL)
        assert h.path == "gs://bucket/path/to/file.txt"

    def test_s3_url(self):
        # HandleAWSURL runs `aws s3api head-object` in __init__; mock to avoid needing AWS CLI
        with patch.object(HandleAWSURL, "__init__", return_value=None):
            h = get_file_handler("s3://bucket/key")
        assert isinstance(h, HandleAWSURL)

    def test_drs_url(self):
        # HandleDRSURI calls drshub API in __init__; mock to avoid needing GCP credentials
        with patch.object(HandleDRSURI, "__init__", return_value=None):
            h = get_file_handler("drs://drs.server.org/object-id")
        assert isinstance(h, HandleDRSURI)

    _GDC_UUID = "550e8400-e29b-41d4-a716-446655440000"

    def test_gdc_api_url(self):
        # HandleGDCHTTPURL calls drshub/GDC API in __init__; mock to avoid live requests
        with patch.object(HandleGDCHTTPURL, "__init__", return_value=None):
            h = get_file_handler(f"https://api.gdc.cancer.gov/data/{self._GDC_UUID}")
        assert isinstance(h, HandleGDCHTTPURL)

    def test_gdc_awg_url(self):
        with patch.object(HandleGDCHTTPURL, "__init__", return_value=None):
            h = get_file_handler(f"https://api.awg.gdc.cancer.gov/data/{self._GDC_UUID}")
        assert isinstance(h, HandleGDCHTTPURL)

    def test_gcs_signed_url_googleapis(self):
        h = get_file_handler("https://storage.googleapis.com/bucket/file.txt")
        assert isinstance(h, HandleGCSSignedURL)

    def test_gcs_signed_url_cloud_google(self):
        h = get_file_handler("https://storage.cloud.google.com/bucket/file.txt")
        assert isinstance(h, HandleGCSSignedURL)

    def test_rodisk_url(self):
        h = get_file_handler("rodisk://canine-crc32c-abc/file.txt")
        assert isinstance(h, HandleRODISKURL)

    def test_generic_https_url(self):
        # HandleOtherURL runs `curl -sIL` in __init__; mock to avoid network
        with patch.object(HandleOtherURL, "__init__", return_value=None):
            h = get_file_handler("https://example.com/data.tar.gz")
        assert isinstance(h, HandleOtherURL)

    def test_generic_http_url(self):
        with patch.object(HandleOtherURL, "__init__", return_value=None):
            h = get_file_handler("http://example.com/data.tar.gz")
        assert isinstance(h, HandleOtherURL)

    def test_ftp_url(self):
        with patch.object(HandleOtherURL, "__init__", return_value=None):
            h = get_file_handler("ftp://ftp.example.org/file.vcf")
        assert isinstance(h, HandleOtherURL)

    def test_plain_string_becomes_string_literal(self):
        h = get_file_handler("just_a_filename.txt")
        assert isinstance(h, StringLiteral)
        assert h.path == "just_a_filename.txt"

    def test_local_file_returns_regular_file_handler(self):
        with patch("os.path.exists", return_value=True):
            from canine.localization.file_handlers import HandleRegularFile
            h = get_file_handler("/local/path/file.txt")
            assert isinstance(h, HandleRegularFile)

    def test_path_coerced_to_string(self):
        # Non-string path-like objects should be str()'d
        h = get_file_handler(42)
        assert isinstance(h, StringLiteral)
        assert h.path == "42"
