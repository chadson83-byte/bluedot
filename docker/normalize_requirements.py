# -*- coding: utf-8 -*-
"""Fly/Docker: Windows UTF-16 requirements.txt -> UTF-8 for pip."""
import pathlib

src = pathlib.Path("/tmp/requirements.txt")
out = pathlib.Path("/app/requirements.txt")
b = src.read_bytes()

if b.startswith(b"\xff\xfe"):
    text = b[2:].decode("utf-16-le")
elif b.startswith(b"\xfe\xff"):
    text = b[2:].decode("utf-16-be")
elif len(b) >= 4 and b[1] == 0 and b[3] == 0:
    text = b.decode("utf-16-le")
else:
    text = b.decode("utf-8-sig")

out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(text.strip() + "\n", encoding="utf-8", newline="\n")
