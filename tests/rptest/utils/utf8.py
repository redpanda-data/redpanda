# List of every control character except the null character
CONTROL_CHARS = ''.join((chr(x) for x in range(1, 32))) + '\x7f'
# List of control character numerical value
CONTROL_CHARS_VALS = [ord(x) for x in CONTROL_CHARS]

# This will create a map that will translate control characters into
# the unicode symbols that represent said control characters
CONTROL_CHARS_MAP = dict(
    zip(CONTROL_CHARS_VALS,
        [(lambda c: b'\xe2\x90\xa1'.decode('utf-8')
          if c == 0x7f else bytes([0xe2, 0x90, 0x80 + c]).decode('utf-8'))(c)
         for c in CONTROL_CHARS_VALS]))
