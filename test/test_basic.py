import unittest


class BasicTestCase(unittest.TestCase):

    def test_import(self):
        import montoux_athena
        self.assertIsInstance(montoux_athena.__version__, str)


if __name__ == '__main__':
    unittest.main()
