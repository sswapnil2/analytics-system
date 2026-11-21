import json
from pathlib import Path
import sys
import unittest

_TESTS_DIR = str(Path(__file__).resolve().parent)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from _bootstrap import REPO_ROOT  # noqa: F401
from pulse_agent._xxh3 import xxh3_64


class TestXxh3(unittest.TestCase):
    def test_empty_vector(self) -> None:
        self.assertEqual(f"{xxh3_64(b''):016x}", "2d06800538d394c2")

    def test_bucketing_vectors_json(self) -> None:
        vec_path = REPO_ROOT / "sdks" / "bucketing_vectors.json"
        with vec_path.open("r", encoding="utf-8") as f:
            vectors = json.load(f)
        self.assertEqual(len(vectors), 12)
        for vec in vectors:
            key = f"{vec['flag_key']}:{vec['distinct_id']}"
            h = xxh3_64(key.encode("utf-8"))
            self.assertEqual(f"{h:016x}", vec["xxh3_rollout_hex"])
            self.assertEqual(h % 10000, vec["rollout_slot"])

    def test_boundary_lengths_and_input_types(self) -> None:
        lengths = [0, 1, 3, 4, 8, 9, 16, 17, 32, 33, 64, 65, 96, 97, 128, 129, 144, 240, 241, 1024, 2048]
        for length in lengths:
            raw = bytes((i * 31 + 7) & 0xFF for i in range(length))
            h_bytes = xxh3_64(raw)
            h_ba = xxh3_64(bytearray(raw))
            h_mv = xxh3_64(memoryview(raw))
            self.assertEqual(h_bytes, h_ba)
            self.assertEqual(h_bytes, h_mv)
            self.assertGreaterEqual(h_bytes, 0)
            self.assertLess(h_bytes, 1 << 64)


if __name__ == "__main__":
    unittest.main()
