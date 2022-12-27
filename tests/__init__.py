from typing import Any

import shortuuid


def get_random_name(*args: Any) -> str:
    prefix = ["test"]
    for item in args:
        prefix.append(item)
    prefix.append(shortuuid.uuid())

    return ".".join(prefix)
