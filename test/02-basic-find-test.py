
import time

import mango
import user_docs


def mkdb():
    return mango.Database("127.0.0.1", "5984", "mango_test")


def setup():
    db = mkdb()
    db.recreate()
    time.sleep(1)
    db.save_docs(user_docs.DOCS)
    indexes = [
        ["user_id"],
        ["name.last", "name.first"],
        ["age"],
        [
            "location.state",
            "location.city",
            "location.address.street",
            "location.address.number"
        ],
        ["company", "manager"],
        ["manager"],
        ["favorites"],
        ["favorites.3"],
        ["twitter"]
    ]
    for idx in indexes:
        assert db.create_index(idx) is True


def test_bad_selector():
    db = mkdb()
    bad_selectors = [
        None,
        True,
        False,
        1.0,
        "foobarbaz",
        {"foo":{"$not_an_op": 2}},
        {"$gt":2},
        [None, "bing"]
    ]
    for bs in bad_selectors:
        try:
            db.find(bs)
        except Exception, e:
            assert e.response.status_code == 400
        else:
            raise AssertionError("bad find")


def test_bad_limit():
    db = mkdb()
    bad_limits = [
        None,
        True,
        False,
        1.2,
        "no limit!",
        {"foo": "bar"},
        [2]
    ],
    for bl in bad_limits:
        try:
            db.find({"int":{"$gt":2}}, limit=bl)
        except Exception, e:
            assert e.response.status_code == 400
        else:
            raise AssertionError("bad find")


def test_bad_skip():
    db = mkdb()
    bad_skips = [
        None,
        True,
        False,
        1.2,
        "no limit!",
        {"foo": "bar"},
        [2]
    ],
    for bs in bad_skips:
        try:
            db.find({"int":{"$gt":2}}, skip=bs)
        except Exception, e:
            assert e.response.status_code == 400
        else:
            raise AssertionError("bad find")


def test_bad_sort():
    db = mkdb()
    bad_sorts = [
        None,
        True,
        False,
        1.2,
        "no limit!",
        {"foo": "bar"},
        [2],
        [{"foo":"asc", "bar": "asc"}],
        [{"foo":"asc"}, {"bar":"desc"}],
    ],
    for bs in bad_sorts:
        try:
            db.find({"int":{"$gt":2}}, sort=bs)
        except Exception, e:
            assert e.response.status_code == 400
        else:
            raise AssertionError("bad find")


def test_bad_fields():
    db = mkdb()
    bad_fields = [
        None,
        True,
        False,
        1.2,
        "no limit!",
        {"foo": "bar"},
        [2],
        [[]],
        ["foo", 2.0],
    ],
    for bf in bad_fields:
        try:
            db.find({"int":{"$gt":2}}, fields=bf)
        except Exception, e:
            assert e.response.status_code == 400
        else:
            raise AssertionError("bad find")


def test_bad_r():
    db = mkdb()
    bad_rs = [
        None,
        True,
        False,
        1.2,
        "no limit!",
        {"foo": "bar"},
        [2],
    ],
    for br in bad_rs:
        try:
            db.find({"int":{"$gt":2}}, r=br)
        except Exception, e:
            assert e.response.status_code == 400
        else:
            raise AssertionError("bad find")


def test_bad_conflicts():
    db = mkdb()
    bad_conflicts = [
        None,
        1.2,
        "no limit!",
        {"foo": "bar"},
        [2],
    ],
    for bc in bad_conflicts:
        try:
            db.find({"int":{"$gt":2}}, conflicts=bc)
        except Exception, e:
            assert e.response.status_code == 400
        else:
            raise AssertionError("bad find")


def test_simple_find():
    db = mkdb()
    docs = db.find({"age": {"$lt": 35}})
    assert len(docs) == 3
    assert docs[0]["user_id"] == 9
    assert docs[1]["user_id"] == 1
    assert docs[2]["user_id"] == 7


def test_multi_cond_and():
    db = mkdb()
    docs = db.find({"manager": True, "location.city": "Longbranch"})
    assert len(docs) == 1
    assert docs[0]["user_id"] == 7
    

def test_multi_cond_or():
    db = mkdb()
    docs = db.find({
            "$and":[
                {"age":{"$gte": 75}},
                {"$or": [
                    {"name.first": "Mathis"},
                    {"name.first": "Whitley"}
                ]}
            ]
        })
    assert len(docs) == 2
    assert docs[0]["user_id"] == 11
    assert docs[1]["user_id"] == 13


def test_multi_col_idx():
    db = mkdb()
    docs = db.find({
        "location.state": {"$and": [
            {"$gt": "Hawaii"},
            {"$lt": "Maine"}
        ]},
        "location.city": {"$lt": "Longbranch"}
    })
    assert len(docs) == 1
    assert docs[0]["user_id"] == 6


def test_missing_not_indexed():
    db = mkdb()
    docs = db.find({"favorites.3": "C"})
    assert len(docs) == 2
    assert docs[0]["user_id"] == 8
    assert docs[1]["user_id"] == 6

    docs = db.find({"favorites.3": None})
    assert len(docs) == 0

    docs = db.find({"twitter": {"$gt": None}})
    assert len(docs) == 4
    assert docs[0]["user_id"] == 1
    assert docs[1]["user_id"] == 4
    assert docs[2]["user_id"] == 0
    assert docs[3]["user_id"] == 13


def test_limit():
    db = mkdb()
    docs = db.find({"age": {"$gt": 0}})
    assert len(docs) == 15
    for l in [0, 1, 5, 14]:
        docs = db.find({"age": {"$gt": 0}}, limit=l)
        assert len(docs) == l

# skip
# sort
# fields
# r
