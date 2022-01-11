import unittest

import one

class OneTest(unittest.TestCase):
    def test_add(self):
        self.assertEqual(one.add(2,3), 5)
    
    def test_sub(self):
        self.assertEqual(one.sub(4,2),2)

if __name__ == '__main__':
    unittest.main()