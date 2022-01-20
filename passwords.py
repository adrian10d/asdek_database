import hashlib


def hash(plaintext):
    salt = b'\x89\x9b\xeb\x99\xf6\xb7\xfb\x93\xcdY\x97\x86\xcfOp?wBw\x80\x95\xb0\xb1\x0c\xf4\xbb\xd5\xab\xb1\x1a;\x1e'
    plaintext = plaintext.encode()
    digest = hashlib.pbkdf2_hmac('sha384', plaintext, salt, 111198)
    hex_hash = digest.hex()
    return hex_hash
