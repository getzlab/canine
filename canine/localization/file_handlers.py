import abc
import google.cloud.storage
import glob, google_crc32c, json, hashlib, base64, binascii, os, re, shlex, subprocess, threading

from ..utils import sha1_base32

class FileType(abc.ABC):
    """
    Stores properties of and instructions for handling a given file type:
    * localization command
    * size
    * hash
    """
    def __init__(self, path, transport = None, localization_mode = None, **kwargs):
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
        self.transport = transport # currently not used
        self.localization_mode = localization_mode
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
    pass

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
    def get_requester_pays(self) -> bool:
        """
        Returns True if the requested gs:// object or bucket resides in a
        requester pays bucket
        """
        bucket = self.path.split('/')[0]

        ret = subprocess.run('gsutil requesterpays get gs://{}'.format(bucket), shell = True, capture_output = True)
        if ret.returncode == 0 or b'BucketNotFoundException: 404' not in ret.stderr:
           return \
             b'requester pays bucket but no user project provided' in ret.stderr \
             or 'gs://{}: Enabled'.format(bucket).encode() in ret.stdout
        else:
            # Try again ls-ing the object itself
            # sometimes permissions can disallow bucket inspection
            # but allow object inspection
            ret = subprocess.run('gsutil ls gs://{}'.format(self.path), shell = True, capture_output = True)
            return b'requester pays bucket but no user project provided' in ret.stderr

        if ret.returncode == 1 and b'BucketNotFoundException: 404' in ret.stderr:
            canine_logging.error(ret.stderr.decode())
            raise subprocess.CalledProcessError(ret.returncode, "")

    def __init__(self, path, localization_mode = "url", **kwargs):
        super().__init__(path, localization_mode = localization_mode, **kwargs)

        # check if this bucket is requester pays
        self.rp_string = ""
        if self.get_requester_pays():
            if "project" not in self.extra_args:
                raise ValueError(f"File {self.path} resides in a requester-pays bucket but no user project provided")
            self.rp_string = f' -u {self.extra_args["project"]}'

    def _get_size(self):
        output = subprocess.check_output('gsutil du -s {} {}'.format(shlex.quote(self.path.strip("/")), self.rp_string), shell=True).decode()
        return int(output.split()[0])

    def _get_hash(self):
        assert self.path.startswith("gs://")
        res = re.search("^gs://([^/]+)/(.*)$", self.path)
        bucket = res[1]
        blob = res[2]

        # TODO: do we need to pass in user project for requester pays buckets here?
        gcs_cl = gcloud_storage_client()

        # check whether this path exists, and whether it's a directory

        i = 0
        is_dir = False
        for b in gcs_cl.list_blobs(bucket, prefix = blob):
            # if there's more than 1 item in the list, it's a directory, since list_blobs
            # returns both the blob and other blobs within it
            if i >= 1:
                is_dir = True
                break
            i += 1
        exists = not i == 0

        if not exists:
            raise GSFileNotExists("{} does not exist.".format(self.path))

        # if it's a directory, hash the set of CRCs within
        if is_dir:
            files = set()
            for b in gcs_cl.list_blobs(bucket, prefix = blob):
                files.add(b.crc32c)
            return hash_set(files)

        # for backwards compatibility, if it's a file, return the file directly
        # TODO: for cleaner code, we really should just always return a set and hash it
        else:
            return binascii.hexlify(base64.b64decode(b.crc32c)).decode().lower()

    def localization_command(self, dest):
        dest_dir = shlex.quote(os.path.dirname(dest))
        dest_file = shlex.quote(os.path.basename(dest))
        return f'gsutil -o "GSUtil:state_dir={dest_dir}/.gsutil_state_dir" cp -r -n -L "{dest_dir}/.gsutil_manifest" {self.path} {dest_dir}/{dest_file} {self.rp_string}'

class HandleGSURLStream(HandleGSURL):
    # TODO: generate the command to make a FIFO. everything else is the same
    pass

# }}}

## GDC HTTPS URLs {{{
class HandleGDCHTTPURL(FileType):
    pass

# }}}

## Regular files {{{

class HandleRegularFile(FileType):
    localization_mode = "local"

    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)

        # path where file got localized to. needs to be manually updated
        self.localized_path = self.path

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
    # hash will be based on disk hash URL (if present) and/or filename
    # * for single file RODISKS, hash will be disk name
    # * for batch RODISKS, hash will be disk name + filename
    # handler will be command to attach/mount the RODISK

# }}}

def get_file_handler(path, url_map = None, **kwargs):
    url_map = {
      r"^gs://" : HandleGSURL, 
      r"^gdc://" : None,
      r"^rodisk://" : None,
    } if url_map is None else url_map

    # firstly, check if the path is a regular file
    if os.path.exists(path):
        return HandleRegularFile(path, **kwargs)

    # next, consult the mapping of path URL -> handler
    for pat, handler in url_map.items():
        if re.match(pat, path):
            return handler(path, **kwargs)

    # otherwise, assume it's a string literal; use the base class
    return FileType(path, **kwargs)
