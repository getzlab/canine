"""
The rclone workspace path is gone.

It mounted a bucket as the shared workspace via rclone and re-exported it over
NFS. rclone was commented out of the image (Dockerfile step 12), so the branch
raised RuntimeError if ever reached -- and nothing enabled it: no run script set
output_bucket, its wolF entry point.

What it left behind was not inert. `create_filesystem()` sudo-created /mnt/rclone
on every cluster start for nothing, `stop()` tried to remove a
.rclone_mounts_<uuid>.sh whose only writer was the deleted branch, and the whole
thing was the `if` of an if/elif/else -- so deleting it in place would have
silently changed which storage branch runs.

These assertions are on source rather than behaviour because init_storage()
shells out to sudo/docker and create_filesystem() needs a real /mnt.
"""
import inspect

from canine.backends.dockerTransient import DockerTransientImageSlurmBackend


class TestNoRcloneAnywhere:

    def test_init_storage_has_no_rclone_branch(self):
        assert "rclone" not in inspect.getsource(
          DockerTransientImageSlurmBackend.init_storage
        )

    def test_create_filesystem_does_not_make_the_mountpoint(self):
        """It ran `sudo mkdir /mnt/rclone` on every cluster, used by nothing."""
        src = inspect.getsource(DockerTransientImageSlurmBackend.create_filesystem)
        assert "/mnt/rclone" not in src
        assert "/mnt/nfs" in src, "the real NFS mountpoint must still be created"

    def test_stop_does_not_chase_the_orphaned_mount_file(self):
        assert "rclone" not in inspect.getsource(DockerTransientImageSlurmBackend.stop)

    def test_rclone_bucket_is_not_a_kwarg(self):
        assert "rclone_bucket" not in inspect.signature(
          DockerTransientImageSlurmBackend.__init__
        ).parameters


class TestStorageBranchesStillResolve:
    """
    The deleted branch was the `if`; the disk branch was an `elif` and the
    no-disk branch the `else`. Collapsing that wrong would either make the
    disk branch unreachable or make it run unconditionally.
    """

    def test_disk_branch_is_now_the_first_condition(self):
        src = inspect.getsource(DockerTransientImageSlurmBackend.init_storage)
        assert 'if self.config["storage_disk"] is not None:' in src
        assert 'elif self.config["storage_disk"]' not in src

    def test_no_disk_branch_still_creates_the_staging_directory(self):
        """
        storage_disk_size=0 means the staging tree lives on the controller's own
        disk -- but something must still mkdir it, or the free-space check below
        raises FileNotFoundError before a single job runs.
        """
        src = inspect.getsource(DockerTransientImageSlurmBackend.init_storage)
        body = src[src.index('if self.config["storage_disk"] is not None:'):]
        assert "else:" in body
        assert "mkdir -p /mnt/nfs/" in body[body.index("else:"):]


class TestDeadWorkflowBucketKwarg:

    def test_storage_bucket_is_not_a_kwarg(self):
        """
        It backed a per-workflow bucket that the localization path never read,
        and the inherited __enter__ overwrote any caller-supplied value anyway,
        so it could never have had an effect.
        """
        assert "storage_bucket" not in inspect.signature(
          DockerTransientImageSlurmBackend.__init__
        ).parameters
