import logging

import boto3

LOG = logging.getLogger(__name__)


def timestr_to_seconds(time_str):
    if not time_str:
        return time_str

    try:
        parts = [int(t) for t in time_str.split(':')]
    except TypeError:
        raise RuntimeError('timestamp ill formatted')

    if len(parts) == 2:
        ret_val = parts[0] * (60 ** 2) + parts[1] * (60)
    elif len(parts) == 3:
        ret_val = parts[0] * (60 ** 2) + parts[1] * (60) + parts[2]
    else:
        raise NotImplementedError('timestamp not handled')

    return ret_val


#
# Bytes-to-human / human-to-bytes converter.
# Based on: http://goo.gl/kTQMs
# Working with Python 2.x and 3.x.
# Author: Giampaolo Rodola' <g.rodola [AT] gmail [DOT] com>
# License: MIT
#
# see: http://goo.gl/kTQMs
SYMBOLS = {
    'customary': ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'),
    'customary_2': ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_ext': ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa',
                      'zetta', 'iotta'),
    'iec': ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'iec_ext': ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi',
                'zebi', 'yobi'),
}


def human2bytes(s):
    """
    Attempts to guess the string format based on default symbols
    set and return the corresponding bytes as an integer.
    When unable to recognize the format ValueError is raised.
      >>> human2bytes('0 B')
      0
      >>> human2bytes('1 K')
      1024
      >>> human2bytes('1 M')
      1048576
      >>> human2bytes('1 Gi')
      1073741824
      >>> human2bytes('1 tera')
      1099511627776
      >>> human2bytes('0.5kilo')
      512
      >>> human2bytes('0.1  byte')
      0
      >>> human2bytes('1 k')  # k is an alias for K
      1024
      >>> human2bytes('12 foo')
      Traceback (most recent call last):
          ...
      ValueError: can't interpret '12 foo'
    """
    init = s
    num = ''
    while s and s[0:1].isdigit() or s[0:1] == '.':
        num += s[0]
        s = s[1:]
    num = float(num)
    letter = s.strip()
    for name, sset in SYMBOLS.items():
        if letter in sset:
            break
    else:
        if letter == 'k':
            # treat 'k' as an alias for 'K' as per: http://goo.gl/kTQMs
            sset = SYMBOLS['customary_2']
            letter = letter.upper()
        else:
            raise ValueError('can\'t interpret %r' % init)
    prefix = {sset[0]: 1}
    for i, _s in enumerate(sset[1:]):
        prefix[_s] = 1 << (i + 1) * 10
    return int(num * prefix[letter])


def human2decimal(s):
    unit = s[-1]
    val = float(s[:-1])

    if unit == 'K':
        ret_val = int(val * 1000)
    elif unit == 'M':
        ret_val = int(val * 1000000)
    else:
        raise ValueError('Error parsing "%s" into integer.' % s)

    return ret_val


SSM = None


def get_ssm_parameter(name, with_decryption=True):
    global SSM
    if SSM is None:
        SSM = boto3.client('ssm')

    response = SSM.get_parameters(Names=[name], WithDecryption=with_decryption)

    if response:
        try:
            return response['Parameters'][0]['Value']
        except (TypeError, IndexError):
            LOG.error("AWS SSM parameter not found in '%s'", response)
            raise
    raise AttributeError("Key '{}' not found in SSM".format(name))
