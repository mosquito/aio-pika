from io import BytesIO


__doc__ = (
    "Base classes that are extended by low "
    "level AMQP frames and higher level AMQP classes and methods."
)


cdef class AMQPObject:
    __doc__ =  (
        "Base object that is extended by AMQP low "
        "level frames and AMQP classes and methods."
    )

    NAME = 'AMQPObject'
    INDEX = None

    def __repr__(self):
        items = list()

        for key in getattr(self, '__attributes__', ()):
            if key.startswith('_'):
                continue

            items.append('%s=%r' % (key, getattr(self, key)))

        if not items:
            return "<%s>" % self.NAME

        return "<%s(%s)>" % (self.NAME, ", ".join(sorted(items)))


cdef class Class(AMQPObject):
    __doc__ = """Is extended by AMQP classes"""
    NAME = 'Unextended Class'


cdef class Method(AMQPObject):
    __doc__ = """Is extended by AMQP methods"""
    NAME = 'Unextended Method'

    cdef bytes _body
    cdef Properties _properties

    __attributes__ = 'body', 'properties'

    def __cinit__(self, *args, **kwargs):
        self._body = b''
        self._properties = Properties()

    @property
    def body(self):
        return self._body

    @body.setter
    def body(self, bytes value):
        self._body = value

    @property
    def properties(self):
        return self._properties

    @properties.setter
    def properties(self, Properties props):
        self._properties = props

    def decode(self, buffer):
        return self

    def encode(self, buffer=None):
        buffer = buffer or BytesIO()
        return buffer


cdef class AsyncMethod(Method):
    @property
    def synchronous(self):
        return False


cdef class SyncMethod(Method):
    @property
    def synchronous(self):
        return True


cdef class Properties(AMQPObject):
    __doc__ = "Class to encompass message properties (AMQP Basic.Properties)"
    NAME = 'Unextended Properties'
