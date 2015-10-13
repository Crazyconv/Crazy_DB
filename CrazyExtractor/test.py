import unittest


class CrazyTest(unittest.TestCase):
    def test_page_number(self):
        import item_processor
        self.assertEqual(item_processor.get_total_page('10-20'), 11)
        self.assertEqual(item_processor.get_total_page('987'), 1)
        self.assertEqual(item_processor.get_total_page('987-'), 1)
        self.assertEqual(item_processor.get_total_page('987-999'), 13)
        self.assertEqual(item_processor.get_total_page('1-4, 5'), 5)
        self.assertEqual(item_processor.get_total_page('1-4, 3-6'), 8)
        self.assertEqual(item_processor.get_total_page('4,5'), 2)
        self.assertEqual(item_processor.get_total_page('4, 5'), 2)
        self.assertEqual(item_processor.get_total_page('4, 5, 7'), 3)
        self.assertEqual(item_processor.get_total_page('4, 5-6, 7-'), 4)

        self.assertEqual(item_processor.get_total_page('i-iv'), 4)
        self.assertEqual(item_processor.get_total_page('i-iv, 1-5'), 9)
        self.assertEqual(item_processor.get_total_page('i,iv'), 2)


if __name__ == '__main__':
    unittest.main()