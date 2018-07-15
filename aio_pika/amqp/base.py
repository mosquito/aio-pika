from io import BytesIO


__doc__ = (
    "Base classes that are extended by low "
    "level AMQP frames and higher level AMQP classes and methods."
)


class AMQPObject:
    __slots__ = ()
    __doc__ = (
        "Base object that is extended by AMQP low "
        "level frames and AMQP classes and methods."
    )

    NAME = 'AMQPObject'
    INDEX = None

    def __repr__(self):
        items = list()

        for key in getattr(self, '__slots__', ()):
            if key.startswith('_'):
                continue

            items.append('%s=%r' % (key, getattr(self, key)))

        if not items:
            return "<%s>" % self.NAME

        return "<%s(%s)>" % (self.NAME, ", ".join(sorted(items)))


class Class(AMQPObject):
    __slots__ = ()
    __doc__ = (
        "Is extended by AMQP classes"
    )

    NAME = 'Unextended Class'



class Method(AMQPObject):
    __slots__ = ()
    __doc__ = (
        "Is extended by AMQP methods"
    )

    NAME = 'Unextended Method'


    def decode(self, buffer):
        return self

    def encode(self, buffer=None):
        buffer = buffer or BytesIO()
        return buffer


class AsyncMethod(Method):

    __slots__ = ()

    @property
    def synchronous(self):
        return False


class SyncMethod(Method):

    __slots__ = ()

    @property
    def synchronous(self):
        return True


class Properties(AMQPObject):

    __slots__ = ()
    __doc__ = (
        "Class to encompass message properties (AMQP Basic.Properties)"
    )

    NAME = 'Unextended Properties'
