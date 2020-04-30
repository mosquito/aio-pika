import shortuuid


def get_random_name(*args):
    prefix = ["test"]
    for item in args:
        prefix.append(item)
    prefix.append(shortuuid.uuid())

    return ".".join(prefix)
