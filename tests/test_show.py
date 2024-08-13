from showstats.showstats import show_stats


def test_show(sample_df, capsys):
    show_stats(sample_df)
    captured = capsys.readouterr()
    assert "Var. N=500" in captured.out
    assert "float_mean_2" in captured.out
    assert "float_min_7" in captured.out
    show_stats(sample_df, "cat")
    captured = capsys.readouterr()
    assert "Var. N=500" in captured.out
    assert "float_mean_2" not in captured.out
    assert "float_min_7" not in captured.out
    assert "str_col" in captured.out
    assert "enum_col" in captured.out
    assert "categorical_col" in captured.out


def test_namespace(sample_df, capsys):
    sample_df.stats.show()
    captured = capsys.readouterr()
    assert "Var. N=500" in captured.out
    assert "float_mean_2" in captured.out
    assert "float_min_7" in captured.out
    sample_df.stats.show("cat")
    captured = capsys.readouterr()
    assert "Var. N=500" in captured.out
    assert "float_mean_2" not in captured.out
    assert "float_min_7" not in captured.out
    assert "str_col" in captured.out
    assert "enum_col" in captured.out
    assert "categorical_col" in captured.out
