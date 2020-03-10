import unittest
import os
from canine import utils

class TestUnit(unittest.TestCase):
    """
    Runs tests on various utilities
    """

    def test_args(self):
        for i in range(100):
            with self.subTest(test=i):
                args = {
                    **{
                        os.urandom(4).hex(): os.urandom(4).hex()
                        for _ in range(i+1)
                    },
                    **{
                        os.urandom(4).hex(): True
                        for _ in range(i+1)
                    },
                    **{
                        os.urandom(1).hex()[0]: True
                        for _ in range(4)
                    }
                }

                cmd = utils.ArgumentHelper(**args).commandline

                for k, v in args.items():
                    if v is True:
                        if len(k) == 1:
                            self.assertRegex(
                                cmd,
                                r'-(\w*?){}(\w*?)'.format(k)
                            )
                        else:
                            self.assertIn(
                                '--{}'.format(k),
                                cmd
                            )
                    else:
                        self.assertIn(
                            '--{}={}'.format(k,v),
                            cmd
                        )
