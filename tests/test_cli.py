import pytest
from planetarypy.cli import main

def test_main_help(capsys):
    """Test the CLI help output."""
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        main(['-h'])
    captured = capsys.readouterr()
    assert 'usage:' in captured.out
    assert pytest_wrapped_e.value.code == 0

def test_main_default():
    """Test the CLI with default arguments."""
    result = main([])
    assert result == 0

def test_main_with_args():
    """Test the CLI with some arguments."""
    result = main(['arg1', 'arg2'])
    assert result == 0