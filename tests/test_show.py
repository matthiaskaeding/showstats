from showstats.showstats import show_stats


def test_show(sample_df, capsys):
    show_stats(sample_df)
    captured = capsys.readouterr()
    assert "Var. N=500" in captured.out
    assert "float_mean_2" in captured.out
    assert "float_min_-7" in captured.out
    show_stats(sample_df, "cat")
    captured = capsys.readouterr()
    assert "Var. N=500" in captured.out
    assert "float_mean_2" not in captured.out
    assert "float_min_-7" not in captured.out
    assert "str_col" in captured.out
    assert "enum_col" in captured.out
    assert "categorical_col" in captured.out


def test_namespace(sample_df, capsys):
    sample_df.stats_tbl.show()
    captured = capsys.readouterr()
    assert "Var. N=500" in captured.out
    assert "float_mean_2" in captured.out
    assert "float_min_-7" in captured.out
    sample_df.stats_tbl.show("cat")
    captured = capsys.readouterr()
    assert "Var. N=500" in captured.out
    assert "float_mean_2" not in captured.out
    assert "float_min_-7" not in captured.out
    assert "str_col" in captured.out
    assert "enum_col" in captured.out
    assert "categorical_col" in captured.out
    sample_df.select("U", "int_col").stats_tbl.show()
    sample_df.select("categorical_col").stats_tbl.show()


def test_show_empty(sample_df, capsys):
    sample_df.select("U", "int_col").stats_tbl.show("cat")
    captured = capsys.readouterr()
    assert captured.out == "No categorical columns found\n"
    sample_df.select("str_col").stats_tbl.show("num")
    captured = capsys.readouterr()
    assert captured.out == "No numerical columns found\n"
