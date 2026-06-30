import sys
import os

# Ensure src/ is on the path even when pytest.ini pythonpath isn't supported
# (pytest < 7).  Safe to run twice; sys.path deduplication is not automatic
# but the duplicate entry is harmless.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
