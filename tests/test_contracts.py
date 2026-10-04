from ninebar import contracts

from .conftest import ROOT

HDR = "square_id\ttime_interval\tcountry_code\tsms_in\tsms_out\tcall_in\tcall_out\t"
ROW = "1\t1383260400000\t39\t0.1\t0.2\t\t0.3\t"


def _obs(tmp_path, text):
    f = tmp_path / "f.txt"
    f.write_text(text)
    return contracts.observe(f, "\t")


def test_headerless_file_matches_v1(pipe, tmp_path):
    r = contracts.resolve(pipe.con, "cdr_activity", _obs(tmp_path, ROW + "5.0\n"))
    assert r.kind == "match" and r.version.version == "1.0"


def test_extra_column_is_additive_and_registers_minor_version(pipe, tmp_path):
    r = contracts.resolve(pipe.con, "cdr_activity", _obs(tmp_path, HDR + "internet\tnew_col\n" + ROW + "5.0\t7\n"))
    assert r.kind == "additive"
    v = contracts.register_additive(pipe.con, r)
    assert v.version == "1.1" and v.columns[-1]["name"] == "new_col" and v.columns[-1]["type"] == "BIGINT"


def test_rename_is_breaking_and_gets_a_hint(pipe, tmp_path):
    r = contracts.resolve(pipe.con, "cdr_activity", _obs(tmp_path, HDR + "internet_activity\n" + ROW + "5.0\n"))
    assert r.kind == "breaking"
    assert any("probable rename: 'internet' -> 'internet_activity'" in p for p in r.problems)


def test_type_change_is_breaking(pipe, tmp_path):
    bad = "\n".join(f"A{i}\t1383260400000\t39\t0.1\t\t\t\t1.0" for i in range(50)) + "\n"
    r = contracts.resolve(pipe.con, "cdr_activity", _obs(tmp_path, bad))
    assert r.kind == "breaking" and any("type change on 'square_id'" in p for p in r.problems)


def test_migration_makes_the_renamed_feed_resolve(pipe, tmp_path):
    obs = _obs(tmp_path, HDR + "internet_activity\n" + ROW + "5.0\n")
    contracts.apply_migration(pipe.con, ROOT / "contracts" / "migrations" / "0001_cdr_internet_renamed.yaml")
    r = contracts.resolve(pipe.con, "cdr_activity", obs)
    assert r.kind == "match" and r.version.version == "2.0"
    assert r.positions["internet"] == 7
