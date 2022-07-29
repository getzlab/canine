import abc
import google.cloud.storage
import glob, google_crc32c, json, hashlib, base64, binascii, os, re, requests, shlex, subprocess, threading
import pandas as pd

from ..utils import sha1_base32, canine_logging

class FileType(abc.ABC):
    """
    Stores properties of and instructions for handling a given file type:
    * localization command
    * size
    * hash
    """

    localization_mode = None # to be overridden in child classes

    def __init__(self, path, transport = None, **kwargs):
        """
        path: path/URL to file
        transport: Canine transport object for handling local/remote files (currently not used)
        localization_mode: how this file will be handled in localization.job_setup_teardown
          must be one of:
          * url: path is a remote URL that must be handled with a special
                 download command
          * stream: stream remote URL into a FIFO, rather than downloading
          * ro_disk: path is a URL to mount a persistent disk read-only
          * local: path is a local file
          * string: path is a string literal
          - None: path is a string literal (for backwards compatibility)
        """
        self.path = path
        self.localized_path = path # path where file got localized to. needs to be manually updated
        self.transport = transport # currently not used
        self.extra_args = kwargs

        self._size = None
        self._hash = None

    @property
    def size(self):
        """
        Returns size of this file in bytes
        """
        if self._size is None:
            self._size = self._get_size()
        return self._size

    def _get_size(self):
        pass

    @property
    def hash(self):
        """
        Returns a hash for this file
        """
        if self._hash is None:
            self._hash = self._get_hash()
        return self._hash

    def _get_hash(self):
        """
        Base class assume self.path is a string literal
        """
        return sha1_base32(bytearray(self.path, "utf-8"), 4)

    def localization_command(self, dest):
        """
        Returns a command to localize this file
        """
        pass

    def __str__(self):
        """
        Some functions (e.g. orchestrator.make_output_DF) may be passed FileType
        objects, but expect strings corresponding to the file path.
        """
        return self.path

class StringLiteral(FileType):
    """
    Since the base FileType class also works for string literals, alias
    the StringLiteral class for clarification
    """
    localization_mode = "string"

def hash_set(x):
    assert isinstance(x, set)
    x = list(sorted(x))
    return hashlib.md5(json.dumps(x).encode()).hexdigest()

#
# define file type handlers

## Google Cloud Storage {{{

STORAGE_CLIENT = None
storage_client_creation_lock = threading.Lock()

def gcloud_storage_client():
    global STORAGE_CLIENT
    with storage_client_creation_lock:
        if STORAGE_CLIENT is None:
            # this is the expensive operation
            STORAGE_CLIENT = google.cloud.storage.Client()
    return STORAGE_CLIENT

class GSFileNotExists(Exception):
    pass

class HandleGSURL(FileType):
    localization_mode = "url"

    def get_requester_pays(self) -> bool:
        """
        Returns True if the requested gs:// object or bucket resides in a
        requester pays bucket
        """
        bucket = re.match(r"gs://(.*?)/.*", self.path)[1]

        ret = subprocess.run('gsutil requesterpays get gs://{}'.format(bucket), shell = True, capture_output = True)
        if b'requester pays bucket but no user project provided' in ret.stderr:
            return True
        else:
            # Try again ls-ing the object itself
            # sometimes permissions can disallow bucket inspection
            # but allow object inspection
            ret = subprocess.run('gsutil ls {}'.format(self.path), shell = True, capture_output = True)
            return b'requester pays bucket but no user project provided' in ret.stderr

        if ret.returncode == 1 and b'BucketNotFoundException: 404' in ret.stderr:
            canine_logging.error(ret.stderr.decode())
            raise subprocess.CalledProcessError(ret.returncode, "")

    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)

        # remove any trailing slashes, in case path refers to a directory
        self.path = path.rstrip("/")

        # check if this bucket is requester pays
        self.rp_string = ""
        if self.get_requester_pays():
            if "project" not in self.extra_args:
                raise ValueError(f"File {self.path} resides in a requester-pays bucket but no user project provided")
            self.rp_string = f' -u {self.extra_args["project"]}'

        # is this URL a directory?
        self.is_dir = False

    def _get_size(self):
        output = subprocess.check_output('gsutil {} du -s {}'.format(self.rp_string, shlex.quote(self.path.rstrip("/"))), shell=True).decode()
        return int(output.split()[0])

    def _get_hash(self):
        assert self.path.startswith("gs://")
        res = re.search("^gs://([^/]+)/(.*)$", self.path)
        bucket = res[1]
        obj_name = res[2]

        gcs_cl = gcloud_storage_client()

        bucket_obj = google.cloud.storage.Bucket(gcs_cl, bucket, user_project = self.extra_args["project"] if "project" in self.extra_args else None)

        # check whether this path exists, and whether it's a directory
        exists = False
        blob_obj = None
        # list_blobs is completely ignorant of "/" as a delimiter
        # prefix = "dir/b" will list
        # dir/b (may not even exist as a standalone "directory")
        # dir/b/file1
        # dir/b/file2
        # dir/boy
        for b in gcs_cl.list_blobs(bucket_obj, prefix = obj_name):
            if b.name == obj_name:
                exists = True
                blob_obj = b
            # a blob starting with <obj_name>/ is a directory
            if b.name.startswith(obj_name + "/"):
                exists = True
                self.is_dir = True
                blob_obj = b
                break

        if not exists:
            raise GSFileNotExists("{} does not exist.".format(self.path))

        # if it's a directory, hash the set of CRCs within
        if self.is_dir:
            canine_logging.info1(f"Hashing directory {self.path}. This may take some time.")
            files = set()
            for b in gcs_cl.list_blobs(bucket_obj, prefix = obj_name + "/"):
                files.add(b.crc32c)
            return hash_set(files)

        # for backwards compatibility, if it's a file, return the file directly
        # TODO: for cleaner code, we really should just always return a set and hash it
        else:
            return binascii.hexlify(base64.b64decode(blob_obj.crc32c)).decode().lower()

    def localization_command(self, dest):
        dest_dir = shlex.quote(os.path.dirname(dest))
        dest_file = shlex.quote(os.path.basename(dest))
        self.localized_path = os.path.join(dest_dir, dest_file)
        return ("[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; ".format(dest_dir = self.localized_path if self.is_dir else dest_dir)) + f'gsutil {self.rp_string} -o "GSUtil:state_dir={dest_dir}/.gsutil_state_dir" cp -r -n -L "{dest_dir}/.gsutil_manifest" {self.path} {dest_dir}/{dest_file if not self.is_dir else ""}'

class HandleGSURLStream(HandleGSURL):
    localization_mode = "stream"

    def localization_command(self, dest):
        return "\n".join(['gsutil {} ls {} > /dev/null'.format(self.rp_string, shlex.quote(self.path)),
        'if [[ -e {0} ]]; then rm {0}; fi'.format(dest),
        'mkfifo {}'.format(dest),
        "gsutil {} cat {} > {} &".format(
            self.rp_string,
            shlex.quote(self.path),
            dest
        )])

# }}}

## AWS S3 {{{

class HandleAWSURL(FileType):
    localization_mode = "url"

    # TODO: use boto3 API; overhead for calling out to aws shell command might be high
    #       this would also allow us to run on systems that don't have the aws tool installed
    # TODO: support directories

    def __init__(self, path, **kwargs):
        """
        Optional arguments:
        * aws_access_key_id
        * aws_secret_access_key
        * aws_endpoint_url
        """
        super().__init__(path, **kwargs)

        # remove any trailing slashes, in case path refers to a directory
        self.path = path.rstrip("/")

        # keys get passed via environment variable
        self.command_env = {}
        self.command_env["AWS_ACCESS_KEY_ID"] = self.extra_args["aws_access_key_id"] if "aws_access_key_id" in self.extra_args else None 
        self.command_env["AWS_SECRET_ACCESS_KEY"] = self.extra_args["aws_secret_access_key"] if "aws_secret_access_key" in self.extra_args else None 
        self.command_env_str = " ".join([f"{k}={v}" for k, v in self.command_env.items() if v is not None])

        # compute extra arguments for s3 commands
        # TODO: add requester pays check here
        self.aws_endpoint_url = self.extra_args["aws_endpoint_url"] if "aws_endpoint_url" in self.extra_args else None 

        self.s3_extra_args = []
        if self.command_env["AWS_ACCESS_KEY_ID"] is None and self.command_env["AWS_SECRET_ACCESS_KEY"] is None:
            self.s3_extra_args += ["--no-sign-request" ]
        if self.aws_endpoint_url is not None:
            self.s3_extra_args += [f"--endpoint-url {self.aws_endpoint_url}"]
        self.s3_extra_args_str = " ".join(self.s3_extra_args)

        # get header for object
        try:
            res = re.search("^s3://([^/]+)/(.*)$", self.path)
            bucket = res[1]
            obj = res[2]
        except:
            raise ValueError(f"{self.path} is not a valid s3:// URL!")

        head_resp = subprocess.run(
          "{env} aws s3api {extra_args} head-object --bucket {bucket} --key {obj}".format(
            env = self.command_env_str,
            extra_args = self.s3_extra_args_str,
            bucket = bucket,
            obj = obj
          ),
          shell = True,
          capture_output = True
        )

        if head_resp.returncode == 254:
            if b"(404)" in head_resp.stderr:
                # check if it's truly a 404 or a directory; we do not yet support these
                ls_resp = subprocess.run(
                  "{env} aws s3api {extra_args} list-objects-v2 --bucket {bucket} --prefix {obj} --max-items 2".format(
                    env = self.command_env_str,
                    extra_args = self.s3_extra_args_str,
                    bucket = bucket,
                    obj = obj
                  ),
                  shell = True,
                  capture_output = True
                )
                if len(ls_resp.stdout) == 0:
                    raise ValueError(f"Object {self.path} does not exist in bucket!")

                ls_resp_headers = json.loads(ls_resp.stdout)
                if len(ls_resp_headers["Contents"]) > 1:
                    raise ValueError(f"Object {self.path} is a directory; we do not yet support localizing those from s3.")
            elif b"(403)" in head_resp.stderr:
                raise ValueError(f"You do not have permission to access {self.path}!")
            else:
                raise ValueError(f"Error accessing S3 file:\n{head_resp.stderr.decode()}")
        elif head_resp.returncode != 0:
            raise ValueError(f"Unknown AWS S3 error occurred:\n{head_resp.stderr.decode()}")
                
        self.headers = json.loads(head_resp.stdout)

    def _get_hash(self):
        return self.headers["ETag"].replace('"', '')

    def _get_size(self):
        return self.headers["ContentLength"]

    def localization_command(self, dest):
        dest_dir = shlex.quote(os.path.dirname(dest))
        dest_file = shlex.quote(os.path.basename(dest))
        self.localized_path = os.path.join(dest_dir, dest_file)

        return "\n".join([
          f"[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :",
          f"[ -f {self.localized_path} ] && SZ=$(stat --printf '%s' {self.localized_path}) || SZ=0",
          f"if [ $SZ != {self.size} ]; then",
          "{env} aws s3api {extra_args} get-object --bucket {bucket} --key {file} --range \"bytes=$SZ-\" >(cat >> {dest}) > /dev/null".format(
            env = self.command_env_str,
            extra_args = self.s3_extra_args_str,
            bucket = self.path.split("/")[2],
            file = "/".join(self.path.split("/")[3:]),
            dest = self.localized_path
          ),
          "fi"
        ])
        
class HandleAWSURLStream(HandleAWSURL):
    localization_mode = "stream"

    def localization_command(self, dest):
        dest_dir = shlex.quote(os.path.dirname(dest))
        dest_file = shlex.quote(os.path.basename(dest))
        self.localized_path = os.path.join(dest_dir, dest_file)

        return "\n".join([
          f"if [[ -e {0} ]]; then rm {0}; fi".format(dest),
          f"[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :",
          'mkfifo {}'.format(dest),
          "{env} aws s3 {extra_args} cp {url} {path} &".format(
            env = self.command_env_str,
            extra_args = self.s3_extra_args_str,
            url = self.path,
            path = dest
          )
        ])

# }}}

## GDC HTTPS URLs {{{
class HandleGDCHTTPURL(FileType):
    localization_mode = "url"

    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)

        self.token = self.extra_args["token"] if "token" in self.extra_args else None 
        self.token_flag = f'--header  "X-Auth-Token: {self.token}"' if self.token is not None else ''
        self.check_md5 = self.extra_args["check_md5"] if "check_md5" in self.extra_args else False

        # parse URL
        self.url = self.path
        url_parse = re.match(r"^(https://api\.(?:awg\.)?gdc\.cancer\.gov)/(?:files|data)/([0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12})", self.url)
        if url_parse is None:
            raise ValueError("Invalid GDC ID '{}'".format(self.url))

        self.prefix = url_parse[1]
        self.uuid = url_parse[2]

        # the actual filename is encoded in the content-disposition header;
        # save this to self.path
        # since the filesize and hashes are also encoded in the header, populate
        # these fields now
        resp_headers = subprocess.run(
          'curl -s -D - -o /dev/full {token_flag} {file}'.format(
            token_flag = self.token_flag,
            file = self.path
          ),
          shell = True,
          capture_output = True
        )
        try:
            headers = pd.DataFrame(
              [x.split(": ") for x in resp_headers.stdout.decode().split("\r\n")[1:]],
              columns=["header", "value"],
            ).set_index("header")["value"]

            self.path = re.match(".*filename=(.*)$", headers["Content-Disposition"])[1]
            self._size = int(headers["Content-Length"])
            self._hash = headers["Content-MD5"]
        except:
            canine_logging.error("Error parsing GDC filename; see stack trace for details")
            raise 
        self.localized_path = self.path

    def localization_command(self, dest):
        dest_dir = shlex.quote(os.path.dirname(dest))
        dest_file = shlex.quote(os.path.basename(dest))
        self.localized_path = os.path.join(dest_dir, dest_file)
        cmd = []
        if self.token is not None:
            cmd += ["[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; curl -C - -o {path} {token} '{url}'".format(dest_dir = dest_dir, path = self.localized_path, token = self.token_flag, url = self.url)]
        else:
            cmd += ["[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; curl -C - -o {path} '{url}'".format(dest_dir = dest_dir, path = self.localized_path, url = self.url)]

        # ensure that file downloaded properly
        if self.check_md5:
            cmd += [f"[ $(md5sum {self.localized_path} | sed -r 's/  .*$//') == {self.hash} ]"]

        return "\n".join(cmd)

class HandleGDCHTTPURLStream(HandleGDCHTTPURL):
    localization_mode="stream"

    def localization_command(self, dest):
        
        dest_dir = shlex.quote(os.path.dirname(dest))
        dest_file = shlex.quote(os.path.basename(dest))
        self.localized_path = os.path.join(dest_dir, dest_file)
        cmd = []
        
        #clean exisiting file if it exists
        cmd += ['if [[ -e {0} ]]; then rm {0}; fi'.format(dest)]
        
        #create dir if it doesnt exist
        cmd += ["[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :;".format(dest_dir = dest_dir)]
        
        #create fifo object
        cmd += ['mkfifo {}'.format(dest)]
        
        #stream into fifo object
        if self.token is not None:
            cmd += ["curl -C - -o {path} {token} '{url}' &".format(path = self.localized_path, token = self.token_flag, url = self.url)]
        else:
            cmd += ["curl -C - -o {path} '{url}' &".format(dest_dir = dest_dir, path = self.localized_path, url = self.url)]

        return "\n".join(cmd)

# }}}

## Regular files {{{

class HandleRegularFile(FileType):
    localization_mode = "local"

    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)

        # remove any trailing slashes, in case path refers to a directory
        self.path = path.rstrip("/")

    def _get_size(self):
        return os.path.getsize(self.path)

    def _get_hash(self):
        # if Canine-generated checksum exists, use it
        k9_crc = os.path.join(os.path.dirname(self.path), "." + os.path.basename(self.path) + ".crc32c")
        if os.path.exists(k9_crc):
            with open(k9_crc, "r") as f:
                return f.read().rstrip()

        # otherwise, compute it
        hash_alg = google_crc32c.Checksum()
        buffer_size = 8 * 1024

        # check if it's a directory
        isdir = False
        if os.path.isdir(self.path):
            files = glob.iglob(self.path + "/**", recursive = True)
            isdir = True
        else:
            files = [self.path]

        for f in files: 
            if os.path.isdir(f):
                continue

            file_size_MiB = int(os.path.getsize(self.path)/1024**2)

            # if we are hashing a whole directory, output a message for each file
            if isdir:
                canine_logging.info1(f"Hashing file {f} ({file_size_MiB} MiB)")

            ct = 0
            with open(f, "rb") as fp:
                while True:
                    # output message every 100 MiB
                    if ct > 0 and not ct % int(100*1024**2/buffer_size):
                        canine_logging.info1(f"Hashing file {self.path}; {int(buffer_size*ct/1024**2)}/{file_size_MiB} MiB completed")

                    data = fp.read(buffer_size)
                    if not data:
                        break
                    hash_alg.update(data)
                    ct += 1

        return hash_alg.hexdigest().decode().lower()

# }}}

## Read-only disks {{{

class HandleRODISKURL(FileType):
    localization_mode = "ro_disk"

    # file size is unnknowable

    # hash is be based on disk hash URL (if present) and/or filename
    # * for single file RODISKS, hash will be disk name
    # * for batch RODISKS, hash will be disk name + filename
    def _get_hash(self):
        roURL = re.match(r"rodisk://([^/]+)/(.*)", self.path)
        if roURL is None or roURL[2] == "":
            raise ValueError("Invalid RODISK URL specified ({})!".format(self.path))

        # we can only compare RODISK URLs based on the URL string, since
        # actually hashing the contents would entail mounting them.
        # most RODISK URLs will contain a hash of their contents, but
        # if they don't, then we warn the user that we may be inadvertently
        # avoiding
        if not roURL[1].startswith("canine-"):
            canine_logging.warning("RODISK input {} cannot be hashed; this job may be inadvertently avoided.".format(self.path))

        # single file/directory RODISKs will contain the CRC32C of the file(s)
        if roURL[1].startswith("canine-crc32c-"):
            return roURL[1][14:]

        # for BatchLocalDisk multifile RODISKs (or non-hashed URLs), the whole URL
        # serves as a hash for the file 
        return self.path

    # handler will be command to attach/mount the RODISK
    # (currently implemented in base.py)

# }}}

def get_file_handler(path, url_map = None, **kwargs):
    url_map = {
      r"^gs://" : HandleGSURL, 
      r"^s3://" : HandleAWSURL, 
      r"^https://api.gdc.cancer.gov" : HandleGDCHTTPURL,
      r"^https://api.awg.gdc.cancer.gov" : HandleGDCHTTPURL,
      r"^rodisk://" : HandleRODISKURL,
    } if url_map is None else url_map

    # zerothly, if path is already a FileType object, return as-is
    if isinstance(path, FileType):
        return path

    # assume path is a string-like object from here on out
    path = str(path)

    # firstly, check if the path is a regular file
    if os.path.exists(path):
        return HandleRegularFile(path, **kwargs)

    # next, consult the mapping of path URL -> handler
    for pat, handler in url_map.items():
        if re.match(pat, path) is not None:
            return handler(path, **kwargs)

    # otherwise, assume it's a string literal; use the base class
    return StringLiteral(path, **kwargs)
