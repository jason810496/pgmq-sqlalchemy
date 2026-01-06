from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
SOURCE_PATH = PROJECT_ROOT / "pgmq_sqlalchemy"
QUEUE_FILE = SOURCE_PATH / "queue.py"
QUEUE_BACKUP_FILE = SOURCE_PATH / "queue_backup.py"
OPERATION_FILE = SOURCE_PATH / "operation.py"
OPERATION_BACKUP_FILE = SOURCE_PATH / "operation_backup.py"
TESTS_PATH = PROJECT_ROOT / "tests"
TEST_QUEUE_FILE = TESTS_PATH / "test_queue.py"
TEST_QUEUE_BACKUP_FILE = TESTS_PATH / "test_queue_backup.py"
