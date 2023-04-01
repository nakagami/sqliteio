import struct


__all__ = ("varint_and_next_index", "to_varint", "decode_payload", "pack_value_list")


def varint_and_next_index(b, i):
    "Get the value of the first varint and index to trailing bytes"
    if (c := b[i]) < 0x80:
        return c, i + 1
    n = 0
    while (c := b[i]) >= 0x80:
        n = (n << 7) + (c & 0x7f)
        i += 1
    return (n << 7) + c, i + 1


def to_varint(n: int) -> bytearray:
    "Convert from int to varint bytearray"
    varint = []
    if n == 0:
        varint.append(0)

    while n:
        b = n & 0x7f
        varint.append(b)
        n = n >> 7
    varint.reverse()
    for i in range(len(varint) - 1):
        varint[i] |= 0x80

    return bytearray(varint)


def _decode_error(body):
    raise ValueError("decode_record()")


_decoders = {
    0: lambda body: (None, body),
    1: lambda body: (body[0], body[1:]),
    2: lambda body: (int.from_bytes(body[:2], 'big'), body[2:]),
    3: lambda body: (int.from_bytes(body[:3], 'big'), body[3:]),
    4: lambda body: (int.from_bytes(body[:4], 'big'), body[4:]),
    5: lambda body: (int.from_bytes(body[:6], 'big'), body[6:]),
    6: lambda body: (int.from_bytes(body[:8], 'big'), body[8:]),
    7: lambda body: (struct.unpack(">d", body[:8])[0], body[8:]),
    8: lambda body: (0, body),
    9: lambda body: (1, body),
    10: _decode_error,
    11: _decode_error,
}


def decode_payload(payload):
    "Convert a record to value list"
    res = []

    n, i = varint_and_next_index(payload, 0)
    header, body = payload[i:n], payload[n:]

    while header:
        c, i = varint_and_next_index(header, 0)
        header = header[i:]
        if c >= 12:
            if c & 1:
                # string
                ln = (c - 13) >> 1
                v, body = body[:ln].decode('utf-8'), body[ln:]
            else:
                # blob
                ln = (c - 12) >> 1
                v, body = bytes(body[:ln]), body[ln:]
        else:
            v, body = _decoders[c](body)
        res.append(v)
    return res


def _encoder(v):
    if v is None:
        return 0, b''
    elif isinstance(v, int):
        if v == 0:
            return 8, b''
        elif v == 1:
            return 9, b''
        elif v == (v & 0xff):
            return 1, bytes([v])
        elif v == (v & 0xffff):
            return 2, v.to_bytes(2, "big")
        elif v == (v & 0xffffff):
            return 3, v.to_bytes(3, "big")
        elif v == (v & 0xffffffff):
            return 4, v.to_bytes(4, "big")
        elif v == (v & 0xffffffffff):
            return 5, v.to_bytes(6, "big")
        elif v == (v & 0xffffffffffff):
            return 6, v.to_bytes(8, "big")
        else:
            raise ValueError("interger value overflow:{}".format(v))
    elif isinstance(v, float):
        return 7, struct.pack(">d", v)
    elif isinstance(v, (bytes, bytearray)):
        return 12 + len(v) * 2, v
    else:
        # string
        b = v.encode('utf-8')
        return 13 + len(b) * 2, b


def pack_value_list(value_list):
    """Convert from list to recode bytearray
    """
    header = bytearray()
    values = bytearray()
    for value in value_list:
        t, v = _encoder(value)
        header += to_varint(t)
        values += v
    return to_varint(len(header) + 1) + header + values
