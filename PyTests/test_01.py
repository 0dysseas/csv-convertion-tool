from counters_cleanup import convert_target_counters
import pandas as pd
import pandas.util.testing as pdt


class TestRiseConversion:
    def setup_class(self):
        self.l1 = ["F02G024", "F42G312", "F18BG034", "B06B002", "F89G127",
                   "F00G899", "F00GB078"]

    def test_counters_conversion_with_zero_format(self):
        assert convert_target_counters(self.l1[0]) == "2024"

    def test_counters_conversion_without_zero_format(self):
        assert convert_target_counters(self.l1[1]) == "42312"

    def test_counters_conversion_with_letters(self):
        assert convert_target_counters(self.l1[2]) == "18034"

    def test_counters_conversion_multiple_zeros(self):
        assert convert_target_counters(self.l1[3]) == "6002"

    def test_counters_conversion_for_c_letter(self):
        assert convert_target_counters(self.l1[4]) == "89127"

    def test_counters_conversion_double_zero(self):
        assert convert_target_counters(self.l1[5]) == "0899"

    def test_counters_conversion_triple_zero(self):
        assert convert_target_counters(self.l1[6]) == "0078"
