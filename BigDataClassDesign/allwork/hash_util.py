"""hash工具 downloaded"""
FNV64_OFFSET_BASIS = 0xcbf29ce484222325
FNV64_PRIME = 0x100000001b3
MAX_64_INT = 2 ** 64
MAX_32_INT = 2 ** 32


def _fnv64(data):
    """FNV64 HASH算法"""
    assert isinstance(data, str)

    h = FNV64_OFFSET_BASIS
    for byte in data.encode():
        h = (h * FNV64_PRIME) % MAX_64_INT
        h ^= byte
    return abs(h)


def _int_to_bytes(x):
    return x.to_bytes((x.bit_length() + 7) // 8, byteorder='big')


def _bytes_to_int(x):
    return int.from_bytes(x, byteorder='big')


def fingerprint(data, size):
    """计算数据的hash值 并截断"""
    fp = _int_to_bytes(_fnv64(data))
    return _bytes_to_int(fp[:size])


def hash_code(data):
    return abs(hash(data))
