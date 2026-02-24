from dltlint.core import check_expected_type


def test_check_expected_type_int_allows_int():
    f = []
    # Should pass
    check_expected_type(123, "root", "my_int", int, f)
    assert len(f) == 0


def test_check_expected_type_int_allows_digit_string():
    f = []
    # Should pass according to user requirement
    check_expected_type("123", "root", "my_int", int, f)
    assert len(f) == 0


def test_check_expected_type_int_fails_non_digit_string():
    f = []
    # Should fail
    check_expected_type("abc", "root", "my_int", int, f)
    assert len(f) == 1
    assert f[0].code == "DLT102"
    assert "must be an integer" in f[0].message


def test_check_expected_type_int_fails_float():
    f = []
    # Should fail
    check_expected_type(12.34, "root", "my_int", int, f)
    assert len(f) == 1
    assert f[0].code == "DLT102"


def test_check_expected_type_str():
    f = []
    check_expected_type("hello", "root", "my_str", str, f)
    assert len(f) == 0

    f = []
    check_expected_type(123, "root", "my_str", str, f)
    assert len(f) == 1
    assert f[0].code == "DLT100"


def test_check_expected_type_bool():
    f = []
    check_expected_type(True, "root", "my_bool", bool, f)
    assert len(f) == 0

    f = []
    check_expected_type("True", "root", "my_bool", bool, f)
    assert len(f) == 1
    assert f[0].code == "DLT101"
