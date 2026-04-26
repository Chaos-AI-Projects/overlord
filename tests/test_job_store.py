"""Tests for the JSON file-based job store."""

import json
import threading
from pathlib import Path

import pytest

from overlord.job_store import JobStore, _job_path, _jobs_dir
from overlord.models import Job, JobStatus


@pytest.fixture
def store(tmp_path):
    """Create a JobStore backed by a temporary directory."""
    return JobStore(data_dir=tmp_path)


def make_job(**kwargs) -> Job:
    defaults = dict(
        name="test-job",
        cron_expression="*/5 * * * *",
        command="echo hello",
    )
    defaults.update(kwargs)
    return Job(**defaults)


class TestJobStoreCreate:
    def test_create_persists_json_file(self, store, tmp_path):
        job = store.create_job(make_job())
        path = _job_path(_jobs_dir(tmp_path), "test-job")
        assert path.exists()
        data = json.loads(path.read_text())
        assert data["name"] == "test-job"
        assert data["cron_expression"] == "*/5 * * * *"

    def test_create_sets_timestamps(self, store):
        job = store.create_job(make_job())
        assert job.created_at is not None
        assert job.updated_at is not None

    def test_create_duplicate_raises(self, store):
        store.create_job(make_job())
        with pytest.raises(FileExistsError):
            store.create_job(make_job())

    def test_create_with_all_fields(self, store):
        job = make_job(
            exclusive_lock="my-lock",
            timeout_seconds=30,
            max_retries=3,
            retry_delay_seconds=10,
            consumes=["queue-a", "queue-b"],
            queue_name="high-priority",
        )
        created = store.create_job(job)
        fetched = store.get_job_by_name("test-job")
        assert fetched.exclusive_lock == "my-lock"
        assert fetched.timeout_seconds == 30
        assert fetched.max_retries == 3
        assert fetched.retry_delay_seconds == 10
        assert fetched.consumes == ["queue-a", "queue-b"]
        assert fetched.queue_name == "high-priority"


class TestJobStoreGet:
    def test_get_by_name(self, store):
        store.create_job(make_job())
        fetched = store.get_job_by_name("test-job")
        assert fetched is not None
        assert fetched.name == "test-job"
        assert fetched.status == JobStatus.ENABLED

    def test_get_nonexistent(self, store):
        assert store.get_job_by_name("nope") is None


class TestJobStoreList:
    def test_list_all(self, store):
        store.create_job(make_job(name="job-1"))
        store.create_job(make_job(name="job-2", status=JobStatus.DISABLED))
        store.create_job(make_job(name="job-3"))

        all_jobs = store.list_jobs()
        assert len(all_jobs) == 3
        names = {j.name for j in all_jobs}
        assert names == {"job-1", "job-2", "job-3"}

    def test_list_by_status(self, store):
        store.create_job(make_job(name="job-1"))
        store.create_job(make_job(name="job-2", status=JobStatus.DISABLED))
        store.create_job(make_job(name="job-3"))

        enabled = store.list_jobs(status=JobStatus.ENABLED)
        assert len(enabled) == 2
        disabled = store.list_jobs(status=JobStatus.DISABLED)
        assert len(disabled) == 1
        assert disabled[0].name == "job-2"

    def test_list_empty(self, store):
        assert store.list_jobs() == []


class TestJobStoreUpdate:
    def test_update_changes_fields(self, store):
        store.create_job(make_job())
        job = store.get_job_by_name("test-job")
        original_updated = job.updated_at

        job.command = "echo updated"
        job.status = JobStatus.DISABLED
        store.update_job(job)

        fetched = store.get_job_by_name("test-job")
        assert fetched.command == "echo updated"
        assert fetched.status == JobStatus.DISABLED
        assert fetched.updated_at != original_updated

    def test_update_nonexistent_raises(self, store):
        with pytest.raises(FileNotFoundError):
            store.update_job(make_job(name="ghost"))


class TestJobStoreDelete:
    def test_delete_removes_file(self, store, tmp_path):
        store.create_job(make_job())
        path = _job_path(_jobs_dir(tmp_path), "test-job")
        assert path.exists()

        store.delete_job("test-job")
        assert not path.exists()
        assert store.get_job_by_name("test-job") is None

    def test_delete_nonexistent_raises(self, store):
        with pytest.raises(FileNotFoundError):
            store.delete_job("ghost")


class TestJobStoreAtomicWrite:
    def test_no_tmp_files_after_write(self, store, tmp_path):
        """Atomic writes should not leave .tmp files behind."""
        store.create_job(make_job())
        jobs_dir = _jobs_dir(tmp_path)
        tmp_files = [f for f in jobs_dir.iterdir() if f.suffix == ".tmp"]
        assert tmp_files == []


class TestJobStoreConcurrency:
    def test_concurrent_creates(self, store):
        """Multiple threads creating different jobs should not corrupt data."""
        errors = []

        def create_job(name):
            try:
                store.create_job(make_job(name=name))
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=create_job, args=(f"job-{i}",))
            for i in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        jobs = store.list_jobs()
        assert len(jobs) == 10

    def test_concurrent_read_write(self, store):
        """Reads during writes should not see partial data."""
        store.create_job(make_job())
        errors = []

        def update_loop():
            for i in range(20):
                job = store.get_job_by_name("test-job")
                job.command = f"echo iteration-{i}"
                store.update_job(job)

        def read_loop():
            for _ in range(20):
                job = store.get_job_by_name("test-job")
                if job is None:
                    errors.append("Got None during concurrent read")
                elif not job.command.startswith("echo"):
                    errors.append(f"Corrupt read: {job.command}")

        writer = threading.Thread(target=update_loop)
        reader = threading.Thread(target=read_loop)
        writer.start()
        reader.start()
        writer.join()
        reader.join()

        assert errors == []
