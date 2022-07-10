import os
from dataclasses import dataclass
from urllib.parse import urlparse, urljoin
import fs
from furl import furl


def uri_loader(_, uri_string) -> bytes:  # discard 1st arg being self
    uri_parts = furl(uri_string)
    p = uri_parts.pathstr
    dirname = os.path.dirname(p)
    filename = os.path.basename(p)
    uri_parts.path = dirname
    with fs.open_fs(uri_parts.tostr()) as p:
        return p.open(filename, 'rb').read()


if __name__ == '__main__':
    access_token = os.getenv('DROPBOX_ACCESS_TOKEN')
    file_path = os.getenv('FILEPATH')
    u = uri_loader(None, f'dropbox://dropbox.com{file_path}?access_token={access_token}')
    print(len(u))
