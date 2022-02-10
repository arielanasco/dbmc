from manage import Manage

manager = Manage()

def test_push():
   pass

def test_get():
    query = {
        "table"  : "m_city",
        "field"  : ["city_nm","city_cd"],
        "filter" : "city_cd = 473014"
    }
    test_result = manager.get(query)
    assert test_result[0][0] == "沖縄県国頭村"
    assert test_result[0][1] == "473014"

def test_update():
   pass

def test_delete():
   pass

def test_connection():
    assert manager.db.is_connected() == True

def test_connection_close():
    assert manager.close_db == True