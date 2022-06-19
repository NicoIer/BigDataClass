from class_work import CuckooFilter


def test_cuckoo():
    return CuckooFilter(capacity=500, fingerprint_size=4, auto_increase=True)


def test_insert(cuckoo_filter):
    assert cuckoo_filter.insert('fake_insert_value')


def test_full_insert(cuckoo_filter: CuckooFilter):
    cuckoo_filter.insert('你好')
    cuckoo_filter.insert('再见')
    print('你好' in cuckoo_filter)
    print('再见' in cuckoo_filter)
    cuckoo_filter.delete('你好')
    print('你好' in cuckoo_filter)
    print('再见' in cuckoo_filter)


def test_strong(cuckoo_filter: CuckooFilter):
    for _ in range(0, 1000):
        cuckoo_filter.insert(str(_))
    for _ in range(0, 400):
        print(str(_) in cuckoo_filter)


if __name__ == '__main__':
    cuckoo = test_cuckoo()
    # test_full_insert(cuckoo)
    test_strong(cuckoo)
