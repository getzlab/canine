import unittest
import os
from functools import lru_cache
from itertools import product
from canine.adapters.base import ManualAdapter

@lru_cache()
def factorial(x):
    if x <= 1:
        return 1
    return x * factorial(x-1)

class TestUnit(unittest.TestCase):
    """
    Runs tests on various utilities
    """

    def test_arrays(self):
        for test in range(15):
            with self.subTest(test=test):
                raw_inputs = {
                    os.urandom(8).hex(): [os.urandom(8).hex() for _ in range(test+1)]
                    for _ in range(test+1)
                }
                twodkeys = []
                for i in range(test+1):
                    twodkeys.append(os.urandom(8).hex())
                    raw_inputs[twodkeys[-1]] =  [
                        [os.urandom(8).hex() for x in range(y+1)]
                        for y in range(test+1)
                    ]
                commkeys = []
                for i in range(test+1):
                    commkeys.append(os.urandom(8).hex())
                    raw_inputs[commkeys[-1]] = [os.urandom(8).hex() for _ in range(test+1)]

                inputs = ManualAdapter(common_inputs=commkeys).parse_inputs({k:v for k,v in raw_inputs.items()})

                self.assertEqual(test+1, len(inputs))

                for i, jid in enumerate(inputs):
                    for k,v in inputs[jid].items():
                        if k in twodkeys:
                            self.assertIsInstance(v, list)
                            self.assertListEqual(
                                v,
                                raw_inputs[k][i]
                            )
                        elif k in commkeys:
                            self.assertIsInstance(v, list)
                            self.assertListEqual(
                                v,
                                raw_inputs[k]
                            )
                        else:
                            self.assertEqual(
                                v,
                                raw_inputs[k][i]
                            )

    def test_regular(self):
        for test in range(15):
            with self.subTest(test=test):
                raw_inputs = {
                    os.urandom(8).hex(): [os.urandom(8).hex() for _ in range(test+1)]
                    for _ in range(test+1)
                }
                common_key = os.urandom(8).hex()
                while common_key in raw_inputs:
                    common_key = os.urandom(8).hex()
                raw_inputs[common_key] = os.urandom(8).hex()

                inputs = ManualAdapter().parse_inputs({k:v for k,v in raw_inputs.items()})

                self.assertEqual(
                    test+1,
                    len(inputs)
                )
                for i, jid in enumerate(inputs):
                    for k, v in inputs[jid].items():
                        if k != common_key:
                            self.assertEqual(
                                v,
                                raw_inputs[k][i]
                            )
                        else:
                            self.assertEqual(v, raw_inputs[k])

        with self.subTest(test='expected failure'):
            with self.assertRaises(ValueError):
                ManualAdapter().parse_inputs({
                    'i1': [1, 2, 3],
                    'i2': [1, 2],
                    'ic': '1'
                })

    def test_product(self):
        for test in range(5):
            with self.subTest(test=test):
                raw_inputs = {
                    os.urandom(8).hex(): [os.urandom(8).hex() for _ in range(j+1)]
                    for j in range(test+1)
                }
                common_key = os.urandom(8).hex()
                while common_key in raw_inputs:
                    common_key = os.urandom(8).hex()
                raw_inputs[common_key] = os.urandom(8).hex()

                inputs = ManualAdapter(product=True).parse_inputs({k:v for k,v in raw_inputs.items()})

                self.assertEqual(
                    factorial(test+1),
                    len(inputs)
                )
                key_order = [k for k in raw_inputs if k != common_key]
                expectation = {*product([raw_inputs[common_key]], *[raw_inputs[k] for k in key_order])}
                key_order = [common_key] + key_order
                self.assertEqual(len(expectation), len(inputs))
