from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import base64
from Cryptodome.Protocol.KDF import PBKDF2
import os, sys
from resources.dev import config
from src.main.utility.logging_config import *
try:
    key = config.key
    iv = config.iv
    salt = config.salt

    if not (key and iv and salt):
        raise Exception(F"Error while fetching details for key/iv/salt")
except Exception as e:
    print(f"Error occurred. Details: {e}")
    sys.exit(0)

def get_private_key():
    Salt = salt.encode('utf-8')
    kdf = PBKDF2(key, Salt, 32, 1000)  # Derive a 32-byte key
    return kdf

def encrypt(raw):
    raw = raw.encode('utf-8')
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    encrypted_data = cipher.encrypt(pad(raw, AES.block_size))
    return base64.b64encode(encrypted_data).decode('utf-8')

def decrypt(enc):
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    decrypted_data = unpad(cipher.decrypt(base64.b64decode(enc)), AES.block_size)
    return decrypted_data.decode('utf-8')



